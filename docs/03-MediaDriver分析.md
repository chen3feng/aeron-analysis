---
title: MediaDriver 实现分析
nav_order: 4
---

# MediaDriver 实现分析

## 1. 概述

MediaDriver 是 Aeron 的核心运行时，负责管理所有网络 I/O、流控、日志缓冲区分配和客户端管理。它运行在独立进程/线程中，与客户端应用通过**共享内存 IPC** 通信。

MediaDriver 的核心设计是**三线程模型**：

```
┌─────────────────────────────────────────────────────────┐
│                    MediaDriver Context                   │
│  配置所有组件：线程工厂、Idle策略、流控策略、日志工厂等      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Conductor    │  │  Sender      │  │  Receiver    │  │
│  │ (主协调线程)  │  │  (发送线程)   │  │  (接收线程)   │  │
│  │              │  │              │  │              │  │
│  │ 命令处理      │  │ UDP Send     │  │ UDP Recv     │  │
│  │ 资源管理      │  │ NAK 处理     │  │ 数据分发      │  │
│  │ 超时检查      │  │ 流控检查      │  │ Status Msg   │  │
│  │ 命名解析      │  │ 拥塞控制      │  │ NAK 发送     │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │                 │                 │          │
│         │  ┌──────────────┼─────────────────┘          │
│         │  │              │                            │
│    ┌────▼──▼──────────┐   │                            │
│    │  Shared Memory   │◄──┘                            │
│    │  (Ring Buffers,  │                                │
│    │   Counters,      │                                │
│    │   Log Buffers)   │                                │
│    └──────────────────┘                                │
└─────────────────────────────────────────────────────────┘
```

- **Conductor**：处理所有管理操作（创建/销毁 Publication、Subscription），周期性超时检查
- **Sender**：轮询所有 NetworkPublication，发送数据到网络，处理 NAK 和流控
- **Receiver**：接收 UDP 数据，重建 Image，发送 Status Message 和 NAK

---

## 2. MediaDriver 入口

[MediaDriver.java](aeron/aeron-driver/src/main/java/io/aeron/driver/MediaDriver.java) 是驱动的主入口：

```
MediaDriver.launch()
  ├── Context.conclude()              // 完成配置，创建所有组件
  │   ├── 创建 LogFactory             // 日志缓冲区工厂
  │   ├── 创建 CountersManager        // 指标计数器
  │   ├── 创建 Receiver → ReceiverProxy
  │   ├── 创建 Sender → SenderProxy
  │   ├── 创建 DriverConductor        // 核心协调器
  │   └── 创建 Command Queues         // IPC 环形缓冲区
  ├── AgentRunner.startOnThread()     // 启动线程
  │   ├── Conductor Agent 线程
  │   ├── Sender Agent 线程
  │   └── Receiver Agent 线程
  └── 等待 ShutdownSignalBarrier      // 阻塞直到收到关闭信号
```

每个线程都是 `Agent` 接口的实现，由 `AgentRunner` 驱动事件循环：

```java
// Agent 核心接口
public interface Agent {
    int doWork() throws Exception;    // 有工作可做时返回 >0
    String roleName();                 // 线程名称
    void onClose();                    // 关闭回调
}
```

AgentRunner 持续调用 `doWork()`，返回 0 时使用 IdleStrategy 进行退避（spin/yield/sleep）。

### 驱动启动关键代码

[MediaDriver.java:4842](aeron/aeron-driver/src/main/java/io/aeron/driver/MediaDriver.java) 的 `Context.conclude()` 方法 ~600 行，构建所有组件：

```
Context.conclude()
  ├── 验证配置参数
  ├── 创建 SystemCounters               // 系统计数器
  ├── 创建 CountersManager              // 计数器管理器
  ├── 创建 LossReport                   // 丢包报告
  ├── 创建 Ring Buffer:
  │   ├── toDriverCommands              // Client → Driver 命令
  │   ├── toClientsBroadcast            // Driver → Client 事件
  │   └── countersValuesBuffer          // 计数器值
  ├── 创建 Receiver                     // 接收 Agent
  ├── 创建 Sender                       // 发送 Agent
  ├── 创建 DriverConductor              // 协调 Agent
  └── 设置 ShutdownSignalBarrier        // 关闭信号
```

---

## 3. DriverConductor：核心协调器

[DriverConductor.java](aeron/aeron-driver/src/main/java/io/aeron/driver/DriverConductor.java) (3549 行) 是 Aeron 驱动的心脏。它实现 `Agent` 接口，在独立线程中运行事件循环。

### 3.1 管理的资源

从 [DriverConductor.java:204-211](aeron/aeron-driver/src/main/java/io/aeron/driver/DriverConductor.java#L204-L211)：

```
networkPublications      // 网络发布者列表
ipcPublications          // IPC 发布者列表
publicationImages        // 接收到的发布镜像
publicationLinks         // 客户端创建的 Publication 连接
subscriptionLinks        // 客户端创建的 Subscription 连接
counterLinks             // 客户端创建的 Counter 连接
clients                  // 已连接的客户端列表
sendChannelEndpointByChannelMap    // UDP 发送通道
receiveChannelEndpointByChannelMap // UDP 接收通道
```

### 3.2 事件循环 (doWork)

Conductor 的 `doWork()` 主循环：

```
doWork()
  ├── 1. toDriverCommands.read()       // 读取客户端命令
  │   ├── ADD_PUBLICATION              // 创建 Publication
  │   │   ├── 分配 sessionId
  │   │   ├── 创建 Log Buffer (3 term)
  │   │   ├── 创建 NetworkPublication / IpcPublication
  │   │   └── 通过 senderProxy 通知 Sender
  │   ├── REMOVE_PUBLICATION           // 移除 Publication
  │   ├── ADD_SUBSCRIPTION             // 添加订阅
  │   │   ├── 创建 SubscriptionLink
  │   │   ├── 绑定 ReceiveChannelEndpoint
  │   │   └── 通过 receiverProxy 通知 Receiver
  │   └── REMOVE_SUBSCRIPTION          // 移除订阅
  ├── 2. driverCmdQueue.drain()        // 内部命令队列
  │   ├── 定时器到期 → 超时检查
  │   ├── reResolution → DNS 重新解析
  │   └── 其他异步任务
  ├── 3. 超时检查（周期性）
  │   ├── 客户端心跳超时 → 清理
  │   ├── Publication 空闲超时
  │   └── 命名解析缓存过期
  └── 4. endOfLifeResources 清理       // 回收已关闭的资源
```

### 3.3 Proxy 模式

Conductor 与 Sender/Receiver 的通信通过 **Proxy** 模式实现——将操作序列化为命令消息，通过队列发送：

```
DriverConductor
      │
      ├── senderProxy.commandQueue      // OneToOneConcurrentArrayQueue
      │   └── Sender 在 doWork() 中 consume
      │
      └── receiverProxy.commandQueue    // OneToOneConcurrentArrayQueue
          └── Receiver 在 doWork() 中 consume
```

这种设计确保 Conductor、Sender、Receiver 线程**无锁共享状态**，每条命令只被一个线程消费。

---

## 4. Sender：发送线程

[Sender.java](aeron/aeron-driver/src/main/java/io/aeron/driver/Sender.java) (232 行) 实现发送端逻辑：

```
Sender.doWork()
  ├── 1. commandQueue.drain()           // 处理 Conductor 的命令
  │   ├── newNetworkPublication         // 注册新网络发布
  │   ├── removeNetworkPublication      // 移除网络发布
  │   └── 其他管理命令
  ├── 2. 轮询所有 NetworkPublication
  │   ├── send()                        // 从 term buffer 读数据发送
  │   │   ├── 检查 term buffer tail counter
  │   │   ├── 组装 DataHeader 帧
  │   │   ├── UDP send()
  │   │   └── 更新 sender position
  │   ├── onStatusMessage()             // 处理 Status Message
  │   │   ├── 更新 receiver position
  │   │   ├── 检查 NAK → 触发重传
  │   │   └── 更新流控窗口
  │   └── onNakMessage()                // 处理 NAK 消息
  │       └── 从 term buffer 找到对应数据 → 重传
  ├── 3. 发送 NAK 重传
  │   └── RetransmitHandler 处理积压的 NAK
  └── 4. 心跳（周期性）
      └── 发送 Setup 帧（新订阅者同步）
```

### 4.1 Padding 消除伪共享

[Sender.java:32-54](aeron/aeron-driver/src/main/java/io/aeron/driver/Sender.java#L32-L54) 通过继承层次添加 padding 字段来防止 CPU cache line 的伪共享：

```java
class SenderLhsPadding { byte p000...p063; }        // 64 bytes padding
class SenderHotFields extends SenderLhsPadding { ... } // 热字段
class SenderRhsPadding extends SenderHotFields { ... } // 64 bytes padding
```

---

## 5. Receiver：接收线程

[Receiver.java](aeron/aeron-driver/src/main/java/io/aeron/driver/Receiver.java) (365 行) 实现接收端逻辑：

```
Receiver.doWork()
  ├── 1. commandQueue.drain()           // 处理 Conductor 的命令
  │   ├── addSubscription               // 添加订阅（绑定 endpoint）
  │   ├── removeSubscription            // 移除订阅
  │   └── addDestination / removeDestination
  ├── 2. dataTransportPoller.poll()     // 轮询所有 ReceiveChannelEndpoint
  │   └── 对每个 endpoint:
  │       └── UDP recv() → onDataPacket()
  │           ├── 解析 DataHeader (streamId, sessionId, termId, termOffset)
  │           ├── 查找/创建 PublicationImage
  │           ├── 写入 Image 的 term buffer（重建）
  │           ├── 检测 gap → 触发 NAK
  │           └── 更新 subscriber position
  ├── 3. 轮询所有 PublicationImage
  │   └── sendStatusMessage()           // 发送 Status Message
  │       ├── 报告 receiver position (消费进度)
  │       ├── 报告 receiver HWM (窗口上限)
  │       └── 附带 NAK (如有 gap)
  └── 4. 清理超时的 PendingSetup
```

---

## 6. IPC 通信架构

Driver 与 Client 之间通过 **CNC (Command and Control)** 共享内存文件通信：

```
┌────────────────────────────────────────────────┐
│              CNC 共享内存文件                     │
│  ┌──────────────────────────────────────────┐  │
│  │  toDriver Ring Buffer (ManyToOne)        │  │  ← Client → Driver 命令
│  │  ADD_PUBLICATION / REMOVE_PUBLICATION     │  │
│  │  ADD_SUBSCRIPTION / REMOVE_SUBSCRIPTION   │  │
│  ├──────────────────────────────────────────┤  │
│  │  toClients Broadcast Buffer              │  │  ← Driver → Client 事件
│  │  ON_PUBLICATION_READY / ON_SUBSCRIPTION   │  │
│  │  ON_AVAILABLE_IMAGE / ON_UNAVAILABLE_IMAGE│  │
│  ├──────────────────────────────────────────┤  │
│  │  Counters Metadata Buffer                │  │  ← 计数器元数据
│  ├──────────────────────────────────────────┤  │
│  │  Counters Values Buffer                  │  │  ← 计数器值（原子更新）
│  ├──────────────────────────────────────────┤  │
│  │  Error Log Buffer                        │  │  ← 错误日志
│  └──────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────┐  │
│  │  Log Buffers (每个 Publication/Image)     │  │
│  │  ┌──────┬──────┬──────┬──────────┐       │  │
│  │  │Term0 │Term1 │Term2 │ MetaData │       │  │
│  │  └──────┴──────┴──────┴──────────┘       │  │
│  └──────────────────────────────────────────┘  │
└────────────────────────────────────────────────┘
```

详见 [Client API 与 IPC](05-ClientAPI与IPC.md)。

---

## 7. 关键设计决策

| 决策 | 实现 | 说明 |
|------|------|------|
| **三线程模型** | Conductor/Sender/Receiver | 管理、发送、接收职责分离 |
| **Agent 事件循环** | `doWork()` + `AgentRunner` | 统一线程模型，IdleStrategy 可配置 |
| **Proxy 模式解耦** | Queue-based 命令传递 | Conductor 不直接调用 Sender/Receiver |
| **Cache Line Padding** | LhsPadding / RhsPadding | 消除伪共享，提升 Sender 吞吐 |
| **共享内存 IPC** | Memory-Mapped Files | Driver 与 Client 零拷贝通信 |
| **定时器驱动** | `timerCheckDeadlineNs` | 周期性超时检查，避免额外定时器线程 |
| **Shutdown 信号** | `ShutdownSignalBarrier` | 优雅关闭，等待各线程完成当前工作 |

---

## 8. 关键源文件索引

| 文件 | 行数 | 说明 |
|------|------|------|
| [MediaDriver.java](aeron/aeron-driver/src/main/java/io/aeron/driver/MediaDriver.java) | 4842 | 驱动入口 + 配置上下文 |
| [DriverConductor.java](aeron/aeron-driver/src/main/java/io/aeron/driver/DriverConductor.java) | 3549 | 核心协调 Agent |
| [Sender.java](aeron/aeron-driver/src/main/java/io/aeron/driver/Sender.java) | 232 | 发送 Agent |
| [Receiver.java](aeron/aeron-driver/src/main/java/io/aeron/driver/Receiver.java) | 365 | 接收 Agent |
| [DriverConductorProxy.java](aeron/aeron-driver/src/main/java/io/aeron/driver/DriverConductorProxy.java) | ~300 | Sender/Receiver → Conductor 代理 |
| [ClientProxy.java](aeron/aeron-driver/src/main/java/io/aeron/driver/ClientProxy.java) | ~150 | Driver → Client 事件广播代理 |
| [SenderProxy.java](aeron/aeron-driver/src/main/java/io/aeron/driver/SenderProxy.java) | ~200 | Conductor → Sender 命令代理 |
| [ReceiverProxy.java](aeron/aeron-driver/src/main/java/io/aeron/driver/ReceiverProxy.java) | ~200 | Conductor → Receiver 命令代理 |
