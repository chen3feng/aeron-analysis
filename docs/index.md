---
title: 首页
nav_order: 1
---

# Aeron 源码分析

Aeron 是一个高效率、低延迟的可靠消息传输系统，由 Martin Thompson 和 Todd Montgomery 于 2014 年开源。它基于 UDP 单播/多播和共享内存 IPC，在应用层实现可靠性保证。设计目标是成为最低延迟、最高吞吐的可靠消息传输系统。

本项目从源码出发，逐模块拆解 Aeron 的核心机制。

## 文档

| # | 文档 | 内容 |
|---|------|------|
| 1 | [架构概述](01-架构概述) | Aeron 是什么、设计原则、分层架构、一次消息的完整生命周期 |
| 2 | [代码结构分析](02-代码结构分析) | 顶层目录、Gradle 构建系统、Java/C/C++ 代码组织 |
| 3 | [MediaDriver 实现分析](03-MediaDriver分析) | 驱动核心三线程模型、Agent 事件循环、Proxy 模式 |
| 4 | [LogBuffer 与协议](04-LogBuffer与协议) | 三区段 Term Buffer、flyweight 协议帧、帧头布局 |
| 5 | [Client API 与 IPC](05-ClientAPI与IPC) | ClientConductor、Publication/Subscription、共享内存 IPC |
| 6 | [流控与拥塞控制](06-流控与拥塞控制) | Min/Max/Multicast 流控、拥塞控制窗口、NAK 重传 |
| 7 | [Archive 实现分析](07-Archive分析) | Recording/Replay 持久化、Catalog 索引、Segment 文件存储 |
| 8 | [Cluster 实现分析](08-Cluster分析) | Raft 共识、Election 选举状态机(18 状态)、Snapshot |
| 9 | [C 与 C++ 实现](09-C与C++实现) | C Client/Driver 独立实现、与 Java 版本的对应关系 |
| 10 | [性能优化技巧](10-性能优化技巧) | Lock-Free CAS、堆外内存、VarHandle、缓存行对齐、sendmmsg、热路径无分配 |

## 快速开始

```bash
git clone --recurse-submodules https://github.com/chen3feng/aeron-analysis.git
cd aeron-analysis
```

源码通过 git submodule 引入，固定在文档生成时的 commit。在 VS Code 中打开，`Cmd/Ctrl + 点击` 代码引用即可跳转到对应行。

## 特点

- **精确行号**：每个代码引用形如 `[file.java:123](aeron/path/to/file.java#L123)`，可点击直接跳转
- **可验证**：所有行号基于 submodule 中的固定 commit，不会溯源失效
- **架构图**：ASCII 流程图和调用链，无需外部工具即可阅读

## 源码

源码分析基于 [Aeron](https://github.com/aeron-io/aeron) 项目，版本锁定在 commit `27fb77ed971065cfbe424519cb5b9ad672ece942`。

## License

MIT
