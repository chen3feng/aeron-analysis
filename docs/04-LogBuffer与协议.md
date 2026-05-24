---
title: LogBuffer 与协议
nav_order: 5
---

# LogBuffer 与协议

## 1. 核心数据结构：LogBuffer

LogBuffer 是 Aeron 数据平面的核心——消息真正存储和传输的地方。它是一个**内存映射的文件或直接缓冲区**，结构如下：

```
┌──────────────────────────────────────────────────────────────┐
│                     Log Buffer (一个连续内存区域)               │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                    Term 0                               │  │
│  │  offset: 0 .. termLength-1                             │  │
│  │  ┌──────┬──────┬──────┬──────┬──────┬────────────────┐ │  │
│  │  │ msg1 │ msg2 │ msg3 │ msg4 │ gap  │...             │ │  │
│  │  └──────┴──────┴──────┴──────┴──────┴────────────────┘ │  │
│  ├────────────────────────────────────────────────────────┤  │
│  │                    Term 1                               │  │
│  │  offset: termLength .. 2*termLength-1                  │  │
│  ├────────────────────────────────────────────────────────┤  │
│  │                    Term 2                               │  │
│  │  offset: 2*termLength .. 3*termLength-1                │  │
│  ├────────────────────────────────────────────────────────┤  │
│  │                 Log Meta Data                           │  │
│  │  ┌─────────────────────────────────────────────────┐   │  │
│  │  │ term_tail_counters[3]    (3 个 int64)           │   │  │
│  │  │ active_term_count         (int32)               │   │  │
│  │  │ end_of_stream_position    (int64)               │   │  │
│  │  │ is_connected              (int32)               │   │  │
│  │  │ initial_term_id           (int32)               │   │  │
│  │  │ term_length               (int32)               │   │  │
│  │  │ mtu_length                (int32)               │   │  │
│  │  │ page_size                 (int32)               │   │  │
│  │  │ correlation_id            (int64)               │   │  │
│  │  │ type                      (int32)               │   │  │
│  │  │ ... 更多元数据字段                             │   │  │
│  │  └─────────────────────────────────────────────────┘   │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### 1.1 为什么是 3 个 Term？

三区段设计（triple-buffering）是 Aeron 的核心设计决策：

- **1 个 Active Term**：Publisher 正在写入的 term
- **1 个 Dirty Term**：已翻转，但 Sender 可能还需要从中读取（等待 NAK 重传）
- **1 个 Clean Term**：可以被翻转为新的 active term

```
时间线:
  t0: Term0(active)  Term1(dirty)   Term2(clean)
  t1: Term0(dirty)   Term1(active)  Term2(clean)    ← flip 到 Term1
  t2: Term0(clean)   Term1(dirty)   Term2(active)   ← flip 到 Term2
  t3: Term0(active)  Term1(clean)   Term2(dirty)    ← flip 回 Term0
```

Term 大小由 `TERM_MIN_LENGTH` (64KB) 到 `TERM_MAX_LENGTH` (1GB) 可配置，定义在 [LogBufferDescriptor.java:57-63](aeron/aeron-client/src/main/java/io/aeron/logbuffer/LogBufferDescriptor.java#L57-L63)。

---

## 2. Term Buffer 操作

### 2.1 Tail Counter：Lock-Free 写入

每个 term 对应一个 **tail counter**，存储在 metadata 区域的 `TERM_TAIL_COUNTERS_OFFSET` [LogBufferDescriptor.java:97](aeron/aeron-client/src/main/java/io/aeron/logbuffer/LogBufferDescriptor.java#L97)。

```
tail counter 是一个 int64 原子变量:
  ┌──────────────────────────┬────────────┐
  │    termId (高32位)       │  offset (低32位)  │
  └──────────────────────────┴────────────┘

Publisher 写入时:
  rawTailVolatile = load tail counter  ← 获取当前 tail
  termId = rawTail >> 32
  offset = rawTail & 0xFFFFFFFF
  
  新 offset = offset + messageLength (对齐到 FRAME_ALIGNMENT=32B)
  新 tail = (termId << 32) | 新 offset
  
  while (!CAS(tail, rawTail, 新 tail))  ← 原子竞争
```

CAS 操作保证了并发 Publisher 写同一个 term 时的正确性，无需锁。

### 2.2 Position：全局偏移

Aeron 使用 64 位 **position** 在整个流生命周期内唯一标识每个字节位置：

```
position = termId * termLength + termOffset

例如: termId=5, termLength=65536, termOffset=128
  → position = 5 * 65536 + 128 = 327808
```

这意味着 position 是一个**永不回绕**的单调递增计数器（可用 200+ 年在万亿消息/秒的速率下）。

---

## 3. 协议帧格式

所有网络协议帧使用 **Flyweight 模式**——直接在 `UnsafeBuffer` 上按字段偏移读写，零反序列化开销。

### 3.1 通用帧头

[HeaderFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/HeaderFlyweight.java) 定义总帧头：

```
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Frame Length                           |  ← int32
  +---------------------------------------------------------------+
  |  Version    |     Flags     |            Type                 |  ← int8 + int8 + int16
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                       Payload (Type 决定)                     ...
```

帧类型定义 [HeaderFlyweight.java:43-89](aeron/aeron-client/src/main/java/io/aeron/protocol/HeaderFlyweight.java#L43-L89)：

| Type | 值 | 帧类型 | 用途 |
|------|-----|---------|------|
| HDR_TYPE_PAD | 0x00 | Padding | 填充 |
| HDR_TYPE_DATA | 0x01 | Data | 数据传输 |
| HDR_TYPE_NAK | 0x02 | NAK | 否定确认 |
| HDR_TYPE_SM | 0x03 | Status Message | 状态报告 |
| HDR_TYPE_ERR | 0x04 | Error | 错误 |
| HDR_TYPE_SETUP | 0x05 | Setup | 订阅同步 |
| HDR_TYPE_RTTM | 0x06 | RTT Measurement | 往返时间测量 |
| HDR_TYPE_RES | 0x07 | Resolution | 命名解析 |

### 3.2 Data Frame（数据帧）

[DataHeaderFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/DataHeaderFlyweight.java) — 32 字节：

```
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |   Frame Length    | Version| Flags |   Type (0x01)            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Term Offset                            |  ← 在 term 内的偏移
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Session ID                             |  ← 会话标识
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Stream ID                              |  ← 流标识
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         Term ID                               |  ← 所属 term
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                     Reserved Value                            |  ← 保留字段 (int64)
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                          Payload                             ...
```

关键字段偏移定义 [DataHeaderFlyweight.java:83-108](aeron/aeron-client/src/main/java/io/aeron/protocol/DataHeaderFlyweight.java#L83-L108)：

- `TERM_OFFSET_FIELD_OFFSET = 8`
- `SESSION_ID_FIELD_OFFSET = 12`
- `STREAM_ID_FIELD_OFFSET = 16`
- `TERM_ID_FIELD_OFFSET = 20`
- `DATA_OFFSET = 32`（数据载荷起始）

**Message Fragment Flags** [DataHeaderFlyweight.java:43-58](aeron/aeron-client/src/main/java/io/aeron/protocol/DataHeaderFlyweight.java#L43-L58)：

- `BEGIN_FLAG (0x80)` — 消息起始 fragment
- `END_FLAG (0x40)` — 消息结束 fragment
- `EOS_FLAG (0x20)` — End of Stream
- `REVOKED_FLAG (0x10)` — Publication 已撤销

一条大消息可能跨越多个 MTU 大小的 frame，通过 BEGIN/END flag 标识。

### 3.3 Status Message Frame

[StatusMessageFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/StatusMessageFlyweight.java) — 36 字节：

```
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |   Frame Length    | Version| Flags |   Type (0x03)            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Session ID                             |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Stream ID                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                Consumption Term ID                            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |               Consumption Term Offset                         |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                  Receiver Window Length                       |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |              Applied Deadline (RTT/2)                         |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |  Group tag (可选 NAK list)                                    |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 3.4 NAK Frame

[NakFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/NakFlyweight.java)：

```
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |   Frame Length    | Version| Flags |   Type (0x02)            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Session ID                             |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Stream ID                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         Term ID                               |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                      Term Offset (缺失开始)                    |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Gap Length                             |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 3.5 Setup Frame

[SetupFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/SetupFlyweight.java) — 新订阅者同步必要信息：

```
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |   Frame Length    | Version| Flags |   Type (0x05)            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Session ID                             |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Stream ID                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                     Active Term ID                            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                   Initial Term ID                             |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                      Term Length                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                       MTU Length                              |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |             TTL (Time-To-Live, 仅多播)                        |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

---

## 4. 消息在 Term Buffer 中的布局

```
Term Buffer (termLength = 65536):
┌─────────────────────────────────────────────────────────────────┐
│ offset 0:                                                        │
│  ┌───────────────────────┐  msg1 (96 bytes, BEGIN+END)          │
│  │ DataHeader (32B)      │                                      │
│  │ Payload (64B)         │                                      │
│  ├───────────────────────┤  offset 96                           │
│  │ DataHeader (32B)      │  msg2 (160 bytes, BEGIN)             │
│  │ Payload part1 (128B)  │                                      │
│  ├───────────────────────┤  offset 256                          │
│  │ DataHeader (32B)      │  msg2 (160 bytes, END)               │
│  │ Payload part2 (128B)  │                                      │
│  ├───────────────────────┤  offset 416                          │
│  │ ...                   │                                      │
│  └───────────────────────┘                                      │
│                                                                  │
│  FRAME_ALIGNMENT = 32 bytes (缓存行对齐)                         │
└─────────────────────────────────────────────────────────────────┘
```

所有帧对齐到 `FRAME_ALIGNMENT = 32` 字节 [DataHeaderFlyweight.java:31](aeron/aeron-client/src/main/java/io/aeron/protocol/DataHeaderFlyweight.java#L31)，确保 CPU 缓存行对齐。

---

## 5. 关键设计决策

| 决策 | 实现 |
|------|------|
| **三区段循环** | 3 个 term，翻转复用，无需 GC |
| **Position 设计** | 64 位永不回绕，`termId * termLength + offset` |
| **Flyweight 编码** | `UnsafeBuffer` 直接按偏移读写字段 |
| **32 字节对齐** | FRAME_ALIGNMENT，匹配 CPU cache line |
| **CAS tail counter** | Lock-free 多 publisher 写 |
| **MTU 分片** | 大消息通过 BEGIN/END flag 跨多帧传输 |
| **小端字节序** | `LITTLE_ENDIAN`，即 x86 原生序 |

---

## 6. 关键源文件索引

| 文件 | 说明 |
|------|------|
| [LogBufferDescriptor.java](aeron/aeron-client/src/main/java/io/aeron/logbuffer/LogBufferDescriptor.java) | Term Buffer 结构与元数据常量 |
| [HeaderFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/HeaderFlyweight.java) | 通用帧头定义 |
| [DataHeaderFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/DataHeaderFlyweight.java) | 数据帧 flyweight |
| [StatusMessageFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/StatusMessageFlyweight.java) | Status Message 帧 |
| [NakFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/NakFlyweight.java) | NAK 帧 |
| [SetupFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/SetupFlyweight.java) | Setup 帧 |
| [ErrorFlyweight.java](aeron/aeron-client/src/main/java/io/aeron/protocol/ErrorFlyweight.java) | Error 帧 |
| [FrameDescriptor.java](aeron/aeron-client/src/main/java/io/aeron/logbuffer/FrameDescriptor.java) | 帧描述符辅助 |
