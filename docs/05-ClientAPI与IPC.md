---
title: Client API 与 IPC
nav_order: 6
---

# Client API 与 IPC

## 1. 客户端架构

Aeron 客户端是应用与 MediaDriver 之间的桥梁。每个进程通过 **共享内存** 与驱动通信，通过 **Publication** 发送消息，通过 **Subscription** 接收消息。

```
┌────────────────────────────────────────────────────┐
│                  应用程序                            │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │Aeron     │  │ Publication  │  │ Subscription │ │
│  │(门面)     │  │ (发送消息)    │  │ (接收消息)    │ │
│  └────┬─────┘  └──────┬───────┘  └──────┬───────┘ │
│       │               │                 │         │
│  ┌────▼───────────────▼─────────────────▼───────┐  │
│  │           ClientConductor                    │  │
│  │  ┌──────────────────────────────────────┐   │  │
│  │  │  DriverProxy (→ to_driver ring buf) │   │  │
│  │  └──────────────────────────────────────┘   │  │
│  │  ┌──────────────────────────────────────┐   │  │
│  │  │  DriverEventsAdapter                │   │  │
│  │  │  (← to_clients broadcast buf)       │   │  │
│  │  └──────────────────────────────────────┘   │  │
│  └────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────┘
         │                       ▲
         │ toDriverCommands      │ toClientsBroadcast
         │ (RingBuffer)          │ (BroadcastBuffer)
         ▼                       │
┌────────────────────────────────────────────────────┐
│                 共享内存 (CNC 文件)                   │
│           MediaDriver 进程                          │
└────────────────────────────────────────────────────┘
```

---

## 2. Aeron 门面类

[Aeron.java](aeron/aeron-client/src/main/java/io/aeron/Aeron.java) 是客户端的唯一入口。应用通常创建一个 Aeron 实例（通过 `Aeron.Context` 配置），然后通过它创建 Publication 和 Subscription。

```java
// 使用示例
Aeron aeron = Aeron.connect(new Aeron.Context()
    .aeronDirectoryName("/dev/shm/aeron/mydriver"));

Publication pub = aeron.addPublication("aeron:udp?endpoint=localhost:8080", 1001);
Subscription sub = aeron.addSubscription("aeron:udp?endpoint=localhost:8080", 1001);

// 发送
pub.offer(buffer, 0, length);

// 接收
sub.poll(handler, fragmentLimit);
```

### 2.1 核心方法

[Aeron.java 核心 API](aeron/aeron-client/src/main/java/io/aeron/Aeron.java)：

| 方法 | 说明 |
|------|------|
| `addPublication(channel, streamId)` | 创建并发 Publication |
| `addExclusivePublication(channel, streamId)` | 创建独占 Publication（单线程） |
| `addSubscription(channel, streamId)` | 创建 Subscription |
| `addCounter(typeId, keyBuffer, label)` | 创建计数器 |
| `conductor()` | 获取 ClientConductor（poll 驱动） |

### 2.2 Channel URI

Aeron 使用 URI 格式的 channel 配置：

```
aeron:udp?endpoint=192.168.1.10:8080|interface=192.168.1.1|ttl=16
aeron:ipc                          ← 同机 IPC
aeron:udp?endpoint=224.0.1.1:8080  ← UDP 多播
```

通道 URI 由 [ChannelUri.java](aeron/aeron-client/src/main/java/io/aeron/ChannelUri.java) 和 [ChannelUriStringBuilder.java](aeron/aeron-client/src/main/java/io/aeron/ChannelUriStringBuilder.java) 解析。

---

## 3. Publication：消息发送

### 3.1 类层次

```
Publication (抽象基类)
├── ConcurrentPublication     ← 多线程安全发送
└── ExclusivePublication      ← 单线程独占发送（无 CAS 开销）
```

[Publication.java](aeron/aeron-client/src/main/java/io/aeron/Publication.java) 定义核心 API：

### 3.2 offer 操作

发送消息到 term buffer：

```
Publication.offer(buffer, offset, length)
  ├── 检查 isConnected
  ├── 计算消息在 term 中的位置
  │   └── if 当前 term 剩余空间不足 → tryRotate()
  ├── 写入 DataHeader (termOffset, sessionId, streamId, termId)
  ├── 写入 Payload
  ├── CAS 推进 tail counter
  ├── 更新 publisher position
  └── 返回 position 或 BACK_PRESSURED / NOT_CONNECTED / ADMIN_ACTION
```

返回值：
- **> 0**：成功，返回 position
- **BACK_PRESSURED** (-2)：发送太快，等待消费者
- **NOT_CONNECTED** (-1)：无订阅者
- **ADMIN_ACTION** (-3)：需要管理员操作
- **MAX_POSITION_EXCEEDED** (-4)：term 写满但无法翻转

### 3.3 ConcurrentPublication vs ExclusivePublication

- **ConcurrentPublication**：多线程可并发 `offer()`，通过 CAS 原子操作竞争 tail
- **ExclusivePublication**：单线程使用，无需 CAS（通过 volatile 读 + 直接写），更高性能

### 3.4 tryClaim：零拷贝写入

对于大消息，使用 `tryClaim` 直接获取 term buffer 中的一块内存：

```java
BufferClaim claim = new BufferClaim();
if (pub.tryClaim(length, claim) > 0) {
    claim.buffer().putBytes(claim.offset(), data, 0, length);
    claim.commit();  // 推进 tail
}
```

---

## 4. Subscription：消息接收

[Subscription.java](aeron/aeron-client/src/main/java/io/aeron/Subscription.java)：

```
Subscription.poll(handler, fragmentLimit)
  ├── 遍历所有 Image
  │   └── Image.poll(handler, fragmentLimit)
  │       ├── 从 term buffer 读取 frame
  │       ├── 解析 DataHeader → fragment
  │       ├── 调用 handler(callback)
  │       └── 推进 subscriber position
  └── 返回 poll 的 fragment 总数
```

### 4.1 Image

[Image.java](aeron/aeron-client/src/main/java/io/aeron/Image.java) 代表订阅者接收到的**一个 Publication 的镜像**。一个 Subscription 可以绑定多个 Image（来自不同 session），如多播场景中每个发送者对应一个 Image。

```
Image 内部结构:
  ┌──────────────────────────┐
  │  Term Buffer (本地重建)    │  ← 从 UDP 接收后写入
  │  ↓                      │
  │  读取 fragment           │
  │  ↓                      │
  │  handler.onFragment()    │  ← 回调应用
  └──────────────────────────┘
```

### 4.2 FragmentAssembler

[FragmentAssembler.java](aeron/aeron-client/src/main/java/io/aeron/FragmentAssembler.java) 自动将分片消息组装为完整消息（检测 BEGIN/END flag），应用只需处理完整消息。

---

## 5. ClientConductor：客户端协调器

[ClientConductor.java](aeron/aeron-client/src/main/java/io/aeron/ClientConductor.java) 是客户端侧的核心调度器：

```
ClientConductor.doWork()
  ├── 1. 处理 Driver 事件 (DriverEventsAdapter)
  │   ├── ON_PUBLICATION_READY    → 创建 Publication 对象
  │   ├── ON_SUBSCRIPTION_READY   → 创建 Subscription 对象
  │   ├── ON_AVAILABLE_IMAGE      → Subscription 添加 Image
  │   ├── ON_UNAVAILABLE_IMAGE    → Subscription 移除 Image
  │   ├── ON_COUNTER_READY        → 创建 Counter
  │   └── ON_ERROR                → 错误处理
  ├── 2. 周期性超时处理
  │   ├── 检查 driver heartbeat (keepalive)
  │   └── 检查 client liveness timeout
  └── 3. 清理已关闭的资源
```

应用通常通过 `Aeron.conductor().doWork()` 或者 `Aeron.conductor().agentInvoker().invoke()` 手动驱动，也可以在 `IdleStrategy.sleepingMillis()` 的 `agentRunner` 中自动驱动。

---

## 6. IPC 机制：CNC 共享内存

### 6.1 CNC 文件布局

[CncFileDescriptor.java](aeron/aeron-client/src/main/java/io/aeron/CncFileDescriptor.java) 定义 CNC 文件的完整布局：

```
CNC 文件:
┌─────────────────────────────────────────────────────────┐
│  CncFileDescriptor Header (版本、PID、各区域偏移)         │
├─────────────────────────────────────────────────────────┤
│  toDriverCommands      → ManyToOneRingBuffer            │
│                         Client → Driver 命令              │
├─────────────────────────────────────────────────────────┤
│  toClientsBroadcast    → BroadcastBuffer               │
│                         Driver → Client(s) 事件广播       │
├─────────────────────────────────────────────────────────┤
│  countersMetaData      → CountersMetadataBuffer         │
│                         计数器类型/标签                    │
├─────────────────────────────────────────────────────────┤
│  countersValues        → CountersValuesBuffer            │
│                         计数器值（原子更新）                │
├─────────────────────────────────────────────────────────┤
│  errorLog              → DistinctErrorLog               │
│                         错误日志                         │
└─────────────────────────────────────────────────────────┘
```

### 6.2 Command Ring Buffer

Client → Driver 命令通过 `ManyToOneRingBuffer` 传递（多 Client 写，单 Driver 读）：

```
命令格式 (SBE 编码):
  ┌──────────────┬──────┬────────────────────┐
  │  msgTypeId   │ ...  │   specific fields  │
  └──────────────┴──────┴────────────────────┘

命令类型:
  ADD_PUBLICATION     → 请求创建 Publication
  REMOVE_PUBLICATION  → 请求移除 Publication
  ADD_SUBSCRIPTION    → 请求创建 Subscription
  REMOVE_SUBSCRIPTION → 请求移除 Subscription
  ADD_COUNTER        → 请求创建 Counter
  REMOVE_COUNTER     → 请求移除 Counter
  CLIENT_KEEPALIVE   → 客户端心跳
```

### 6.3 Broadcast Buffer

Driver → Client(s) 事件广播通过 `BroadcastBuffer` 传递（单 Driver 写，多 Client 读）：

```
事件类型:
  ON_PUBLICATION_READY      → Publication 创建成功（附 term buffer 信息）
  ON_SUBSCRIPTION_READY     → Subscription 创建成功
  ON_AVAILABLE_IMAGE        → 新的 Image 可用（有 sender 连入）
  ON_UNAVAILABLE_IMAGE      → Image 不可用（sender 断开）
  ON_COUNTER_READY          → Counter 创建成功
  ON_CLIENT_TIMEOUT         → Client 超时
```

### 6.4 Log Buffer：消息数据

每个 Publication 有自己的 Log Buffer（3 term + metadata），直接内存映射。Publisher 直接写入，Subscriber 直接读取，**零拷贝**。

在 IPC 场景中，Log Buffer 直接指向同一块物理内存页（通过 `mmap`），写入后立即可读。

---

## 7. 关键设计决策

| 决策 | 实现 |
|------|------|
| **Client ↔ Driver 解耦** | RingBuffer 命令传递，异步操作 |
| **Broadcast 事件** | Driver 到所有 Client 使用广播缓冲区 |
| **Log Buffer 共享** | Publisher 和 Subscriber 直接读写同一内存映射文件 |
| **手动 Poll 驱动** | `ClientConductor.doWork()` 手动调用，应用控制驱动频率 |
| **CAS vs Volatile** | ConcurrentPublication 用 CAS，Exclusive 用 volatile write |
| **FragmentAssembler** | 自动组装分片消息为完整消息 |

---

## 8. 关键源文件索引

| 文件 | 说明 |
|------|------|
| [Aeron.java](aeron/aeron-client/src/main/java/io/aeron/Aeron.java) | 客户端门面 |
| [Publication.java](aeron/aeron-client/src/main/java/io/aeron/Publication.java) | 消息发送抽象基类 |
| [ConcurrentPublication.java](aeron/aeron-client/src/main/java/io/aeron/ConcurrentPublication.java) | 并发安全 Publication |
| [ExclusivePublication.java](aeron/aeron-client/src/main/java/io/aeron/ExclusivePublication.java) | 独占 Publication |
| [Subscription.java](aeron/aeron-client/src/main/java/io/aeron/Subscription.java) | 消息订阅 |
| [Image.java](aeron/aeron-client/src/main/java/io/aeron/Image.java) | 接收端流镜像 |
| [ClientConductor.java](aeron/aeron-client/src/main/java/io/aeron/ClientConductor.java) | 客户端协调器 |
| [DriverProxy.java](aeron/aeron-client/src/main/java/io/aeron/DriverProxy.java) | Client → Driver 命令代理 |
| [CncFileDescriptor.java](aeron/aeron-client/src/main/java/io/aeron/CncFileDescriptor.java) | CNC 文件布局 |
| [FragmentAssembler.java](aeron/aeron-client/src/main/java/io/aeron/FragmentAssembler.java) | 消息组装 |
| [ChannelUri.java](aeron/aeron-client/src/main/java/io/aeron/ChannelUri.java) | Channel URI 解析 |
