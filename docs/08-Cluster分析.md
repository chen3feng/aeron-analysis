---
title: Cluster 实现分析
nav_order: 9
---

# Cluster 实现分析

## 1. 概述

Aeron Cluster 在 Archive 之上构建了基于 **Raft** 共识算法的容错有状态服务框架。它允许应用以**复制状态机**的形式运行，自动处理 Leader 选举、日志复制、Snapshot 和故障恢复。

模块规模：98 个 Java 文件，约 4 万行代码（[ConsensusModuleAgent.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java) 3617 行，[ConsensusModule.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModule.java) 4644 行）。

## 2. 整体架构

```
┌────────────────────────────────────────────────────────────┐
│                      Cluster Node                          │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │          ClusteredMediaDriver (一站式启动)             │  │
│  │  ┌──────────────┐  ┌──────────┐  ┌──────────────────┐│  │
│  │  │ MediaDriver  │  │ Archive  │  │  ConsensusModule ││  │
│  │  └──────┬───────┘  └────┬─────┘  └────────┬─────────┘│  │
│  └─────────┼───────────────┼─────────────────┼──────────┘  │
│            │               │                  │             │
│  ┌─────────▼───────────────▼──────────────────▼──────────┐  │
│  │                Aeron Channels                          │  │
│  │  ┌──────────┐ ┌───────────┐ ┌──────────┐ ┌─────────┐ │  │
│  │  │ Log      │ │ Consensus │ │ Ingress  │ │ Egress  │ │  │
│  │  │ Channel  │ │ Channel   │ │ Channel  │ │ Channel │ │  │
│  │  │(日志复制)│ │(心跳/投票) │ │(客户端入)│ │(服务出) │ │  │
│  │  └──────────┘ └───────────┘ └──────────┘ └─────────┘ │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘

多节点部署:
  ┌──────────┐   ┌──────────┐   ┌──────────┐
  │ Leader   │   │Follower  │   │Follower  │
  │ Node 0   │   │ Node 1   │   │ Node 2   │
  └──────────┘   └──────────┘   └──────────┘
       │               │              │
       └───────┬───────┴──────┬───────┘
               │              │
        Log Channel (UDP 多播/单播)
        Consensus Channel
```

## 3. ClusteredMediaDriver：一站式启动

[ClusteredMediaDriver.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusteredMediaDriver.java) 组合启动 MediaDriver + Archive + ConsensusModule：

```
ClusteredMediaDriver.launch(ctx)
  ├── 1. MediaDriver.launch()                     // 启动网络驱动
  ├── 2. Archive.launch()                         // 启动归档服务
  └── 3. ConsensusModule.launch()                 // 启动共识模块
      └── 创建 ConsensusModuleAgent (Agent)
          └── AgentRunner.startOnThread()          // 共识线程
```

## 4. ConsensusModule：共识模块入口

[ConsensusModule.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModule.java) (4644 行) 是 Cluster 的顶层门面，类似于 MediaDriver 的角色：

```
ConsensusModule.launch(ctx)
  ├── 配置验证
  ├── 创建 Aeron Client（连接本地 MediaDriver）
  ├── 创建 Archive Client（连接本地 Archive）
  ├── 创建 Mark File（节点状态持久化）
  ├── 创建 RecordingLog（Raft 日志索引）
  ├── 创建 ConsensusModuleAgent
  ├── 创建 TimerService
  ├── 创建 ServiceProxy
  ├── 启动 ConsensusModuleAgent 线程
  └── 启动 ClusteredServiceContainer（应用服务容器）
```

### 4.1 ConsensusModule.State

[ConsensusModule.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModule.java) 定义模块级状态机：

| 状态 | 说明 |
|------|------|
| INIT | 初始化中 |
| ACTIVE | 正常运行（Leader 或 Follower） |
| SUSPENDED | 暂停（Leader 挂起，不接收新请求） |
| SNAPSHOT | 正在制作 Snapshot |
| TERMINATING | 正在终止 |
| CLOSED | 已关闭 |

## 5. ConsensusModuleAgent：共识引擎

[ConsensusModuleAgent.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java) (3617 行) 是实现 Raft 共识的核心 Agent。

### 5.1 关键字段

从 [ConsensusModuleAgent.java:120-206](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java#L120-L206)：

```
leadershipTermId       — 当前 leadership term
commitPosition         — 已提交的日志位置
appendPosition         — 日志追加位置
role                   — Leader / Follower / Candidate
state                  — ACTIVE / SUSPENDED / SNAPSHOT / TERMINATING
leaderMember           — 当前 Leader
activeMembers          — 集群成员列表
election               — 选举状态机（null = 不在选举中）
sessionManager         — 客户端会话管理
recordingLog            — Raft 日志记录
logPublisher            — Raft 日志的 Publisher（Leader 写）
logAdapter              — Raft 日志的 Subscription（Follower 读）
consensusAdapter        — 共识通道消息接收
consensusPublisher      — 共识消息发送
ingressAdapter          — 客户端请求入口
egressPublisher         — 服务响应出口
timerService            — 定时器服务
consensusModuleExtension — 扩展点
```

### 5.2 主事件循环

[ConsensusModuleAgent.java:390-443](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java#L390-L443)：

```
doWork()
  ├── slowTickWork(nowNs)              // 周期性慢任务（10ms 频率）
  │   ├── aeronClientInvoker.invoke()  // 驱动 Aeron 客户端事件
  │   ├── pollArchiveEvents()          // 轮询 Archive 事件（录制信号）
  │   ├── sessionManager 处理           // 重定向、拒绝、会话检查
  │   ├── 心跳超时检查 → enterElection() // Leader 超时触发选举
  │   ├── ClusterControl 处理           // SUSPEND/SNAPSHOT/SHUTDOWN
  │   └── recordingLogValidator 检查
  ├── consensusAdapter.poll()          // 处理共识消息
  │   ├── 心跳 (Heartbeat)
  │   ├── 投票请求 (RequestVote)
  │   ├── 新领导任期 (NewLeadershipTerm)
  │   └── AppendPosition
  ├── election.doWork(nowNs)           // 选举状态机（如果正在进行）
  │   或
  │   consensusWork(timestamp, nowNs)  // 正常共识工作
  │   ├── Leader:
  │   │   ├── timerService.poll()      // 定时器处理
  │   │   ├── pendingServiceMessageTracker.poll()
  │   │   ├── ingressAdapter.poll()    // 处理客户端请求
  │   │   │   └── 追加到 Raft log → logPublisher
  │   │   └── updateLeaderPosition()   // 更新 Leader 位置
  │   └── Follower:
  │       ├── logAdapter.poll()        // 读取 Leader 复制的日志
  │       ├── commitPosition 更新
  │       └── updateFollowerPosition() // 上报 Follower 位置
  ├── consensusModuleAdapter.poll()    // 处理模块内部事件
  ├── pollStandbySnapshotReplication()
  └── clusterTimeConsumer 回调         // 应用层时间推进
```

### 5.3 slowTickWork（慢周期任务）

[ConsensusModuleAgent.java:2318-2413](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java#L2318-L2413)：

```
slowTickWork(nowNs)  ← 每 10ms 执行一次
  ├── aeronClientInvoker.invoke()            — 驱动 Aeron 客户端
  ├── markFile.updateActivityTimestamp()     — 更新活跃时间戳
  ├── pollArchiveEvents()                    — 轮询录制事件
  ├── sessionManager:
  │   ├── sendRedirects()                    — 非 Leader 重定向
  │   ├── sendRejections()                    — 认证拒绝
  │   ├── processAllPendingSessions()         — Leader 处理待处理会话
  │   └── checkSessions()                     — 会话超时/心跳检查
  ├── 心跳检测:
  │   ├── Leader: 检查 follower quorum 是否活跃
  │   └── Follower: 检查 leader 心跳是否超时
  │       └── 超时 → enterElection()
  ├── ClusterControl 检查:
  │   ├── SUSPEND   → 挂起
  │   ├── SNAPSHOT  → 触发快照
  │   ├── SHUTDOWN  → 优雅关闭流程
  │   └── ABORT     → 强制关闭
  ├── nodeControlToggle 检查
  └── recordingLogValidator 检查
```

### 5.4 consensusWork

[ConsensusModuleAgent.java:2415-2477](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java#L2415-L2477)：

**Leader 模式：**
```
consensusWork() [Leader]
  ├── timerService.poll(timestamp)           — 处理到期的定时器
  ├── pendingServiceMessageTracker.poll()    — 待处理服务消息
  ├── ingressAdapter.poll()                  — 客户端请求
  │   └── 每条请求:
  │       ├── 验证 session
  │       ├── 追加到 LogPublisher (Raft 日志)
  │       └── 通过 Log Channel 复制到 Followers
  └── updateLeaderPosition(nowNs)
      ├── 检查 follower 的 append position
      ├── 计算 quorum → 推进 commitPosition
      └── 通知 serviceProxy 新 commit 的条目
```

**Follower 模式：**
```
consensusWork() [Follower]
  ├── 检查是否到达 terminationPosition
  ├── logAdapter.poll(limit)
  │   └── 从 Log Channel 读取 Leader 复制的条目
  │       ├── 追加到本地 RecordingLog (Archive)
  │       └── 推进本地 position
  ├── commitPosition.proposeMaxRelease(logAdapter.position())
  ├── ingressAdapter.poll()  — 处理来自其他 node 的消息
  └── updateFollowerPosition(nowNs)
      └── 通过 Consensus Channel 上报 appendPosition 给 Leader
```

## 6. Election：Leader 选举

[Election.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/Election.java) (1643 行) 实现完整的 Raft 选举协议。

### 6.1 ElectionState 状态机

[ElectionState.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ElectionState.java) 定义 18 个状态：

```
INIT (0)
  ↓
CANVASS (1)          ← 探测集群成员状态
  ↓
NOMINATE (2)         ← 发起投票请求
  ↓
CANDIDATE_BALLOT (3) ← 等待投票结果 (Candidate)
  ├──→ LEADER_LOG_REPLICATION (5)    ← 赢得选举
  │     ↓
  │   LEADER_REPLAY (6)              ← 重放本地日志
  │     ↓
  │   LEADER_INIT (7)                ← 初始化 Leader 状态
  │     ↓
  │   LEADER_READY (8)               ← Leader 就绪，发布新任期
  │     ↓
  └──→ CLOSED (17)                  ← 新 Leader 已确立

FOLLOWER_BALLOT (4)                  ← 投票给他人后等待
  ├──→ FOLLOWER_LOG_REPLICATION (9) ← 需要追日志
  │     ↓
  │   FOLLOWER_REPLAY (10)          ← 重放到当前
  │     ↓
  │   FOLLOWER_CATCHUP_INIT (11)    ← 追赶 (catchup)
  │     ↓
  │   FOLLOWER_CATCHUP_AWAIT (12)   ← 等待 catchup replay
  │     ↓
  │   FOLLOWER_CATCHUP (13)         ← catchup 合并
  │     ↓
  │   FOLLOWER_LOG_INIT (14)        ← 初始化日志连接
  │     ↓
  │   FOLLOWER_LOG_AWAIT (15)       ← 等待加入 live log
  │     ↓
  │   FOLLOWER_READY (16)           ← Follower 就绪
  │     ↓
  └──→ CLOSED (17)                  ← 选举结束
```

### 6.2 选举触发

选举由以下任一事件触发 [ConsensusModuleAgent.java:2354-2385](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java#L2354-L2385)：

- **Leader 心跳超时**（Follower 在 `leaderHeartbeatTimeoutNs` 内未收到 Leader 消息）
- **Follower quorum 丢失**（Leader 发现活跃 Follower 不足半数）
- **Log Channel 断开**（EOS 信号）
- **手动触发**（ClusterControl）

```
enterElection(isEos, reason)
  ├── 创建 Election 对象
  ├── 重置 election 状态为 INIT
  └── 下一轮 doWork() 中 election.doWork() 接管控制
```

### 6.3 选举流程

```
Election.doWork()
  ├── INIT:
  │   └── 检查是否需要选举 → CANVASS
  ├── CANVASS:
  │   └── 探测所有成员的最新 log position 和 leadership term
  ├── NOMINATE:
  │   └── 发送 VoteRequest（含 term + log position）
  ├── CANDIDATE_BALLOT:
  │   └── 收集投票，达到 quorum → LEADER_LOG_REPLICATION
  ├── FOLLOWER_BALLOT:
  │   └── 投票后等待新 Leader 的 NewLeadershipTerm 消息
  ├── LEADER_LOG_REPLICATION → LEADER_REPLAY → LEADER_INIT → LEADER_READY
  └── FOLLOWER_LOG_REPLICATION → ... → FOLLOWER_READY
```

**投票条件**（Raft 标准）：
```
grantVote = candidateTerm >= myTerm
         && candidateLogPosition >= myLogPosition
```

## 7. 多通道架构

每个 Cluster 节点使用 4 个 Aeron Channel（实际是 4 个 streamId 组）：

| Channel | 用途 | 方向 | 可靠性 |
|---------|------|------|--------|
| **Log** | Raft 日志复制（Leader → Followers） | Leader → Follower | 可靠（NAK 重传） |
| **Consensus** | 心跳、投票请求、AppendPosition | 双向 | 可靠 |
| **Ingress** | 客户端 → Cluster 请求 | Client → All nodes | 可靠 |
| **Egress** | Cluster → 客户端响应 | Leader → Client | 可靠 |

```
Client
  │
  │ Ingress (aeron:udp?endpoint=...:9010)
  ├──→ Node 0 (Leader)  ← 处理请求
  ├──→ Node 1 (Follower) ← 转发给 Leader
  └──→ Node 2 (Follower) ← 转发给 Leader

Node 0 (Leader)
  │
  │ Log Channel (aeron:udp?endpoint=...:9020)
  ├──→ Node 1 (Follower) ← 日志复制
  └──→ Node 2 (Follower) ← 日志复制

  │ Consensus Channel (aeron:udp?endpoint=...:9030)
  ├──→ Node 1 ← 心跳 + 投票
  └──→ Node 2 ← 心跳 + 投票

  │ Egress (响应客户端)
  └──→ Client
```

## 8. SessionManager：客户端会话管理

[SessionManager.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/SessionManager.java) 管理所有连接到 Cluster 的客户端会话：

```
SessionManager 职责:
  ├── 会话生命周期管理（CREATE → CONNECT → AUTHENTICATED → CLOSE）
  ├── 非 Leader 重定向（返回 leader endpoint）
  ├── 认证（通过 Authenticator）
  ├── 授权（通过 AuthorisationService）
  ├── 会话心跳检测
  └── Standby snapshot 通知
```

### 8.1 ClusterSession 状态机

[ClusterSession.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusterSession.java)：

| 状态 | 说明 |
|------|------|
| INIT | 初始化 |
| CONNECTING | 正在连接 |
| CONNECTED | 已连接，等待认证 |
| CHALLENGED | 认证质询中 |
| AUTHENTICATED | 已认证，可发送请求 |
| REJECTED | 认证/授权拒绝 |
| CLOSING | 正在关闭 |
| INVALID | 无效（超时等） |

### 8.2 请求处理

```
Client 请求 → IngressAdapter.poll()
  ├── 查找/创建 ClusterSession
  ├── 认证（如未认证）
  │   └── 非 Leader → redirect to leader
  ├── 授权检查
  └── 追加到 Log:
      └── LogPublisher.appendMessage()
          ├── 写入 term buffer
          └── 日志自动复制到 Followers
             （通过 Aeron Log Channel 的多播）
```

## 9. RecordingLog：Raft 日志持久化

[RecordingLog.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/RecordingLog.java) (1790 行) 通过 Archive 的 Recording 来持久化 Raft 日志条目。

### 9.1 日志结构

```
Raft Log:
┌──────────────────────────────────────────────────────────┐
│  [Snapshot at term 5, pos 1000000]                        │
│  ───────────────────────────────────── (截断线)           │
│  [Term 6 entries]  pos 1000000 → 1005000                 │
│  [Term 7 entries]  pos 1005000 → 1010000                 │
│  [Term 8 entries]  pos 1010000 → 1012000 (current)      │
└──────────────────────────────────────────────────────────┘

每个 Term 对应一个 Archive Recording:
  recordingId → Archive Recording → segment files
```

### 9.2 RecoveryPlan

启动时，`RecordingLog` 根据记录的 snapshot + term recordings 生成恢复计划：

```
RecoveryPlan:
  ├── snapshotRecordingId          — 最新的 snapshot
  ├── snapshotStopPosition         — snapshot 结束位置
  ├── activeRecordingId            — 当前活跃的 term recording
  └── recoveryPosition             — 恢复起始位置
```

Leader/Follower 启动时根据 RecoveryPlan 决定重放哪些日志条目。

### 9.3 Snapshot 与截断

Snapshot 是整个应用程序状态的快照。制作后，snapshot 之前的日志条目可以被截断（删除），减少恢复时间：

```
Snapshot 流程:
  1. ClusterControl.SNAPSHOT 触发
  2. Leader 追加 ClusterAction.SNAPSHOT 到 log
  3. 状态变为 SNAPSHOT
  4. 调用应用 onTakeSnapshot()
  5. 应用通过 Archive 录制 snapshot
  6. snapshot 完成通知 Agent
  7. Agent 调用 recordingLog.commitLogPosition()
  8. 截断 snapshot 之前的 term recordings
  9. 状态恢复为 ACTIVE
```

[ConsensusModuleSnapshotTaker.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleSnapshotTaker.java) 协调 snapshot 过程，[StandbySnapshotReplicator.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/StandbySnapshotReplicator.java) 将 leader snapshot 复制到 standby 节点。

## 10. TimerService

[TimerService.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/TimerService.java) 提供集群级定时器，实现可故障转移的定时任务。定时器的创建和触发通过 Raft 日志复制，所有节点一致执行。

两种实现：
- **PriorityHeapTimerService**：基于优先队列，O(log n) 插入/删除
- **WheelTimerService**：基于时间轮，O(1) 插入

## 11. ClusterBackup：备份节点

[ClusterBackupAgent.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusterBackupAgent.java) (1160 行) 是不参与 Leader 选举的观察者节点：

- 同步 Leader 的日志和 snapshot
- 不参与 quorum 投票
- 可用于只读查询、离线分析
- 支持 Standby 模式：自动从备份切换为活跃节点

## 12. 日志复制完整流程

```
Client
  │
  │ (1) 发送请求到 Ingress Channel
  ▼
Leader ConsensusModuleAgent
  │
  │ (2) ingressAdapter.poll() 接收请求
  │     验证 session + auth
  │
  │ (3) logPublisher.appendMessage()
  │     追加到 Log Channel 的 term buffer
  │     同时通过 Archive 录制到本地
  │
  │ (4) Log Channel (Aeron UDP 多播)
  ├────→ Follower 1 收到
  ├────→ Follower 2 收到
  │
  ▼
Follower ConsensusModuleAgent
  │
  │ (5) logAdapter.poll() 读取条目
  │     追加到本地 Archive Recording
  │
  │ (6) 通过 Consensus Channel 上报 appendPosition
  │     (告知 Leader 自己复制到哪里了)
  │
  ▼
Leader ConsensusModuleAgent
  │
  │ (7) updateLeaderPosition()
  │     检查所有 follower 的 appendPosition
  │     计算 quorum: 多数 follower 已确认
  │
  │ (8) 推进 commitPosition
  │     通知 serviceProxy.commitPosition()
  │
  │ (9) 应用层执行已提交的条目
  │
  │ (10) egressPublisher.sendResponse()
  │      发送响应到 Egress Channel
  ▼
Client ← 接收响应
```

## 13. 关键设计决策

| 决策 | 实现 |
|------|------|
| **Raft 共识** | 成熟、易理解的分布式共识算法 |
| **Archive 作为日志后端** | 复用 Aeron 已有的持久化能力 |
| **多 Channel 隔离** | Log/Consensus/Ingress/Egress 独立 channel 互不干扰 |
| **Snapshot 截断** | 减少启动恢复的日志重放量 |
| **Leader 统一入口** | 所有客户端请求和定时器只在 Leader 处理 |
| **Follower 转发** | Follower 收到客户端请求自动重定向到 Leader |
| **Backup/Standby** | 不参与投票的观察者角色，支持只读和热切换 |
| **ClusterServiceExtension** | 应用通过扩展接口插入自定义逻辑 |

## 14. 关键源文件索引

| 文件 | 行数 | 说明 |
|------|------|------|
| [ClusteredMediaDriver.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusteredMediaDriver.java) | ~300 | 一站式启动 |
| [ConsensusModule.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModule.java) | 4644 | 共识模块入口 + 配置 |
| [ConsensusModuleAgent.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java) | 3617 | **核心**：共识引擎事件循环 |
| [Election.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/Election.java) | 1643 | Leader 选举状态机 |
| [ElectionState.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ElectionState.java) | 172 | 18 种选举状态枚举 |
| [RecordingLog.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/RecordingLog.java) | 1790 | Raft 日志持久化 + RecoveryPlan |
| [ClusterBackupAgent.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusterBackupAgent.java) | 1160 | 备份节点 |
| [SessionManager.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/SessionManager.java) | ~500 | 客户端会话管理 |
| [ClusterSession.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusterSession.java) | ~200 | 会话状态机 |
| [ServiceProxy.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ServiceProxy.java) | ~400 | 应用层回调代理 |
| [ConsensusPublisher.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusPublisher.java) | ~300 | 共识消息 Publisher |
| [LogPublisher.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/LogPublisher.java) | ~200 | 日志 Publisher |
| [IngressAdapter.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/IngressAdapter.java) | ~200 | 客户端入站适配器 |
| [EgressPublisher.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/EgressPublisher.java) | ~150 | 服务出站 Publisher |
| [ConsensusModuleSnapshotTaker.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleSnapshotTaker.java) | ~200 | Snapshot 协调 |
| [StandbySnapshotReplicator.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/StandbySnapshotReplicator.java) | ~200 | Standby Snapshot 复制 |
| [ClusterTermination.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusterTermination.java) | ~200 | 优雅关闭 |
| [ClusterControl.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusterControl.java) | ~100 | 集群控制状态 |
| [ClusterMember.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusterMember.java) | ~200 | 成员信息 |
| [ClusterMembership.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/ClusterMembership.java) | ~100 | 成员动态变化 |
| [TimerService.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/TimerService.java) | ~50 | 定时器接口 |
| [PriorityHeapTimerService.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/PriorityHeapTimerService.java) | ~200 | 优先队列定时器 |
| [WheelTimerService.java](aeron/aeron-cluster/src/main/java/io/aeron/cluster/WheelTimerService.java) | ~200 | 时间轮定时器 |
