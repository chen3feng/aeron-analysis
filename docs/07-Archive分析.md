---
title: Archive 实现分析
nav_order: 8
---

# Archive 实现分析

## 1. 概述

Archive 是 Aeron 的可持久化子系统，允许将消息流**录制到磁盘**并**按需回放**。它本身作为 Aeron 的客户端运行——通过标准的 Subscription 消费实时数据流，然后持久化到文件系统；通过 Publication 从文件读取数据并重新发布。

Archive 在 Cluster 系统中充当关键角色：**Raft 日志的后端存储**和**Snapshot 存储**。

## 2. 核心概念

```
Publisher ──→ Aeron Channel ──→ Subscriber(s)  ← 实时消费
                 │
                 ├──→ RecordingSession ──→ archive/<recordingId>/*.rec  ← 录制到磁盘
                 │
                 └──→ ReplaySession ←── archive/<recordingId>/*.rec      ← 从磁盘回放
                      (通过新 Publication 发布)
```

三个核心操作：

- **Recording**：订阅一个已有的 Aeron stream，将所有数据帧写入 .rec 文件
- **Replay**：从 .rec 文件读取数据，通过新的 Publication 重新发布到指定 channel
- **Catalog**：索引所有录制的元数据（channel、streamId、sessionId、起止 position、时间戳）

## 3. 架构

```
┌──────────────────────────────────────────────────────────┐
│                   Archive 进程                            │
├──────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────┐  │
│  │              ArchiveConductor (Agent)               │  │
│  │  ┌──────────────┐  ┌─────────────┐  ┌────────────┐ │  │
│  │  │Recording     │  │ Replay      │  │ Control    │ │  │
│  │  │Session Pool  │  │ Session Pool│  │ Sessions   │ │  │
│  │  └──────┬───────┘  └──────┬──────┘  └─────┬──────┘ │  │
│  │         │                │               │        │  │
│  │  ┌──────▼────────────────▼───────────────▼──────┐  │  │
│  │  │             Catalog (录制索引)                │  │  │
│  │  │  ┌──────────────────────────────────────┐   │  │  │
│  │  │  │ RecordingDescriptor[]                │   │  │  │
│  │  │  │ (recordingId, channel, streamId,     │   │  │  │
│  │  │  │  startPosition, stopPosition, ...)   │   │  │  │
│  │  │  └──────────────────────────────────────┘   │  │  │
│  │  └────────────────────────────────────────────┘  │  │
│  ├────────────────────────────────────────────────────┤  │
│  │              Storage (录制文件)                     │  │
│  │  archive/<recordingId>/                            │  │
│  │    ├── segment-0000000000.rec                      │  │
│  │    ├── segment-0000000001.rec                      │  │
│  │    └── ...                                         │  │
│  ├────────────────────────────────────────────────────┤  │
│  │  ArchiveMarkFile (启动校验 + 错误日志)               │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Aeron Client (用于收/发录制数据, 收发控制指令)       │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

## 4. 核心组件详解

### 4.1 ArchiveConductor

[ArchiveConductor.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ArchiveConductor.java) 是 Archive 的核心 Agent，运行在独立线程中：

```
ArchiveConductor.doWork()
  ├── 1. 处理客户端控制请求（通过 Aeron  subscription 接收）
  │   ├── START_RECORDING       → 创建 RecordingSession
  │   │   ├── 从 Catalog 分配 recordingId
  │   │   ├── 订阅目标 channel/streamId
  │   │   └── 创建 RecordingWriter
  │   ├── STOP_RECORDING        → 停止并关闭 RecordingSession
  │   ├── START_REPLAY          → 创建 ReplaySession
  │   │   ├── 从 Catalog 查找 recordingId
  │   │   ├── 打开 .rec 文件
  │   │   └── 通过 Publication 回放
  │   ├── LIST_RECORDINGS       → 查询 Catalog
  │   ├── EXTEND_RECORDING      → 追加录制
  │   └── REPLICATE_RECORDING   → 跨 Archive 复制
  ├── 2. 轮询 RecordingSession 列表
  │   ├── 读取 Subscription 数据
  │   └── 写入 .rec segment 文件
  ├── 3. 轮询 ReplaySession 列表
  │   ├── 从 .rec 文件读取
  │   └── 通过 Publication.offer() 发布
  ├── 4. 错误处理与重试
  └── 5. 周期性任务
      ├── Catalog 持久化
      ├── 超时检查（会话空闲超时）
      └── Segment 文件轮转
```

#### 线程模式

[ArchiveThreadingMode.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ArchiveThreadingMode.java) 定义两种模式：

| 模式 | 说明 |
|------|------|
| **DEDICATED** | ArchiveConductor 运行在独立线程，专用于 Archive |
| **SHARED** | 与 MediaDriver 的 Conductor 共享线程 |

`DedicatedModeArchiveConductor` 在专用线程中运行，实现更低的 Archive 操作延迟。

### 4.2 Catalog

[Catalog.java](aeron/aeron-archive/src/main/java/io/aeron/archive/Catalog.java) 管理录制索引：

```
Catalog 数据结构:
┌─────────────────────────────────────────────┐
│  Header                                     │
│  ├── version                                │
│  ├── nextRecordingId                         │
│  └── recordingCount                          │
├─────────────────────────────────────────────┤
│  RecordingDescriptor[] (固定大小数组)         │
│  ├── [0] recordingId, channel, streamId...  │
│  ├── [1] ...                                │
│  └── [N] invalid (free slot)                │
└─────────────────────────────────────────────┘

Catalog 存储在内存映射文件中，每次更新后 fsync。
```

关键操作：
- `addNewRecording()` — 分配 recordingId + 写入 descriptor
- `onStopRecording()` — 更新 stopPosition
- `findRecording()` — 按 recordingId 查找
- `listRecordings()` — 支持按 channel/streamId 过滤

### 4.3 RecordingSession

[RecordingSession.java](aeron/aeron-archive/src/main/java/io/aeron/archive/RecordingSession.java) — 录制会话：

```
RecordingSession 生命周期:
  1. INIT         → 初始化，订阅目标 channel/streamId
  2. RECORDING    → 从 Subscription poll 数据，写入 .rec 文件
  3. STOPPING     → 接收到停止命令
  4. STOPPED      → 停止录制，更新 Catalog
```

**RecordingWriter** [RecordingWriter.java](aeron/aeron-archive/src/main/java/io/aeron/archive/RecordingWriter.java) 负责将数据写入 .rec 文件。Segment 文件在达到 `SEGMENT_FILE_LENGTH`（默认 128MB）时自动轮转。

### 4.4 ReplaySession

[ReplaySession.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ReplaySession.java) — 回放会话：

```
ReplaySession 流程:
  1. 从 Catalog 获取录制信息
  2. 打开指定 recordingId 的 .rec segment 文件
  3. 定位到起始 position
  4. 循环读取 frame → Publication.offer() 发布
  5. 支持限速（replayRateLimit）
  6. 支持边界 replay（指定起始 position 和长度）
```

### 4.5 ControlSession

[ControlSession.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ControlSession.java) — 客户端通过 Aeron channel 连接 Archive 发送控制命令：

```
Client ──controlRequest──→ Archive ControlSession
Client ←──controlResponse── Archive ControlResponseProxy
```

控制命令包括 startRecording、stopRecording、replay、listRecordings 等。

## 5. Recording 文件格式

```
archive/<recordingId>/
├── segment-0000000000.rec   ← position [0, 128MB)
├── segment-0000000001.rec   ← position [128MB, 256MB)
├── segment-0000000002.rec   ← position [256MB, 384MB)
└── ...

每个 .rec 文件内容:
  ┌────────────────┐
  │ DataHeader (32)│  ← termOffset, sessionId, streamId, termId
  ├────────────────┤
  │ Payload        │
  ├────────────────┤
  │ DataHeader (32)│
  ├────────────────┤
  │ Payload        │
  └────────────────┘
  ... 连续的 header + payload frames
```

- Segment 文件大小默认 128MB（可通过 `SEGMENT_FILE_LENGTH` 配置）
- 文件内容与 Term Buffer 中的数据帧格式完全一致
- Replay 时可直接将文件内容映射到 `UnsafeBuffer` 进行零拷贝读取

## 6. Archive 迁移

Aeron Archive 支持多版本存储格式，通过 [ArchiveMigrationPlanner.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ArchiveMigrationPlanner.java) 和系列 `ArchiveMigration_X_Y.java` 实现格式升级。这确保了旧版本的录制数据在新版本 Archive 中可读。

## 7. 关键设计决策

| 决策 | 实现 |
|------|------|
| **基于 Aeron 本身** | 录制和回放全部通过标准 Publication/Subscription API |
| **Segment 分片** | 避免单文件过大，便于删除和迁移 |
| **内存映射 Catalog** | Catalog 文件 mmap，快速读写 + 崩溃安全 |
| **独立线程模式** | Archive 可完全隔离，不干扰驱动性能 |
| **RecordingDescriptor 固定大小** | 简化 Catalog 管理，O(1) 查找 |

## 8. 关键源文件索引

| 文件 | 说明 |
|------|------|
| [Archive.java](aeron/aeron-archive/src/main/java/io/aeron/archive/Archive.java) | Archive 入口 |
| [ArchiveConductor.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ArchiveConductor.java) | Archive 核心协调器 |
| [ArchivingMediaDriver.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ArchivingMediaDriver.java) | 一键启动 Driver+Archive |
| [Catalog.java](aeron/aeron-archive/src/main/java/io/aeron/archive/Catalog.java) | 录制索引 |
| [CatalogIndex.java](aeron/aeron-archive/src/main/java/io/aeron/archive/CatalogIndex.java) | Catalog 快速查找索引 |
| [RecordingSession.java](aeron/aeron-archive/src/main/java/io/aeron/archive/RecordingSession.java) | 录制会话 |
| [RecordingWriter.java](aeron/aeron-archive/src/main/java/io/aeron/archive/RecordingWriter.java) | Segment 文件写入 |
| [ReplaySession.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ReplaySession.java) | 回放会话 |
| [ControlSession.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ControlSession.java) | Archive 控制会话 |
| [ArchiveThreadingMode.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ArchiveThreadingMode.java) | 线程模式 |
| [ArchiveMigrationPlanner.java](aeron/aeron-archive/src/main/java/io/aeron/archive/ArchiveMigrationPlanner.java) | 格式迁移 |
