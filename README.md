# aeron-analysis

## 项目背景

在金融交易、实时数据分发和高频消息系统中，传统的 TCP 消息中间件面临三个根本瓶颈：

- **队头阻塞**：TCP 保证有序交付，一条消息丢失会阻塞后续所有消息，在微秒级场景中不可接受。
- **内核态开销**：Socket read/write 涉及系统调用和内存拷贝，吞吐量受限于内核态/用户态切换。
- **连接模型僵化**：TCP 是一对一连接，多播需要维护多条连接，状态管理复杂。

[Aeron](https://github.com/aeron-io/aeron) 于 2014 年由 Martin Thompson 和 Todd Montgomery 开源，设计目标是成为最低延迟、最高吞吐的可靠消息传输系统。其核心思想是：

- **UDP + 应用层可靠性**：基于 UDP 单播/多播实现无连接消息传输，通过 NAK（Negative Acknowledgment）协议在应用层保证可靠性，消除队头阻塞。
- **零拷贝共享内存 IPC**：同一机器上的进程间通信直接通过内存映射文件进行，完全绕过网络栈。
- **Lock-Free 设计**：所有热路径数据结构无锁，基于 CAS 原子操作，消除上下文切换开销。

在 Aeron 使用场景中，单机 IPC 吞吐可达 **数千万消息/秒**，UDP 网络上也能达到 **数百万消息/秒**，延迟在微秒级别。Aeron 已被全球顶尖金融机构、交易所和高频交易公司在生产系统中大规模使用。其创始人 Martin Thompson 也是 LMAX Disruptor 的作者，Aeron 继承了 Disruptor 的 lock-free 和 mechanical sympathy 设计哲学。

Aeron 项目源码规模为 Java 约 9 万行 + C 约 4 万行（含客户端与驱动完整实现）+ C++ 示例，架构精炼但概念密集（lock-free 数据结构、flyweight 协议编码、三线程驱动模型、Raft 共识）。官方 Wiki 侧重于使用和配置，对内部实现细节覆盖有限。

## 本项目做什么

从源码出发，逐模块拆解 Aeron 的核心机制：

- **不是官方文档的翻译**——官方 Wiki 已有的概念说明不再重复
- **不是论文的复述**——聚焦于代码层面的具体实现
- **可点击直达源码行号**——每个代码引用链接到 submodule 中固定 commit 的精确行
- **由浅入深**——从架构全景逐步深入到 term buffer 的原子操作细节

## 快速开始

```bash
git clone --recurse-submodules https://github.com/chen3feng/aeron-analysis.git
cd aeron-analysis
```

源码通过 git submodule 引入，固定在文档生成时的 commit。在 VS Code 中打开，`Cmd/Ctrl + 点击` 代码引用即可跳转到对应行。

## 文档索引

| # | 文档 | 内容 |
|---|------|------|
| 1 | [架构概述](docs/01-架构概述.md) | Aeron 是什么、设计原则、分层架构、一次消息的完整生命周期、各子系统概览 |
| 2 | [代码结构分析](docs/02-代码结构分析.md) | 顶层目录、Gradle 构建系统、Java/C/C++ 代码组织、模块依赖关系 |
| 3 | [MediaDriver 实现分析](docs/03-MediaDriver分析.md) | 驱动核心三线程模型：Conductor/Sender/Receiver、Agent 事件循环、Proxy 模式 |
| 4 | [LogBuffer 与协议](docs/04-LogBuffer与协议.md) | 三区段 Term Buffer、flyweight 协议帧、Data/Status/NAK 消息、帧头布局 |
| 5 | [Client API 与 IPC](docs/05-ClientAPI与IPC.md) | ClientConductor、Publication/Subscription、共享内存 IPC、Driver-Client 通信 |
| 6 | [流控与拥塞控制](docs/06-流控与拥塞控制.md) | Min/Max/Multicast 流控、发送端/接收端限流、拥塞控制窗口、NAK 重传 |
| 7 | [Archive 实现分析](docs/07-Archive分析.md) | Recording/Replay 持久化、Catalog 索引、ControlSession、Segment 文件存储 |
| 8 | [Cluster 实现分析](docs/08-Cluster分析.md) | Raft 共识、Election 选举状态机(18 状态)、日志复制、多通道架构、Snapshot |
| 9 | [C 与 C++ 实现](docs/09-C与C++实现.md) | C Client/Driver 独立实现、与 Java 版本的对应关系、C++ 客户端示例 |
| 10 | [性能优化技巧](docs/10-性能优化技巧.md) | Lock-Free CAS、堆外内存、VarHandle、缓存行对齐、sendmmsg/recvmmsg、Flyweight、热路径无分配 |

## 文档特点

- **精确行号**：每个代码引用形如 `[file.java:123](aeron/path/to/file.java#L123)`，可点击直接跳转
- **可验证**：所有行号基于 submodule 中的固定 commit，不会溯源失效
- **架构图**：ASCII 流程图和调用链，无需外部工具即可阅读
- **表格总结**：每个模块末尾有关键设计决策和行号对照表

## 这些文档是如何生成的

### 探索

按照人类分析代码的路径，系统性地阅读代码：

1. **确定分析角度**——每篇文档围绕一个核心问题（"MediaDriver 如何调度？""Term Buffer 如何实现零拷贝？"）
2. **逐层探索**——从入口点开始，跟踪调用链；找到关键类和函数后，读取具体实现
3. **回到源头**——文档中的行号都是通过 grep/read 精确查找得到的，不是推测

### 生成

探索完成后，整理出结构化的文档：

1. **代码引用可点击**——每个类名、函数名、关键常量后附带 `[file:line](path#L行号)` 格式的链接
2. **架构图**——生成 ASCII 流程图、调用链、层次关系图
3. **交叉引用**——文档间互相链接，形成体系

### 版本锁定

`aeron/` 作为 git submodule，固定在文档生成时的 commit `27fb77ed971065cfbe424519cb5b9ad672ece942`。这保证了：

- 所有行号永久有效
- 查看者可以在 GitHub 上看到**文档引用的那个版本的**源码
- 后续更新时，只需更新 submodule 指针并检查差异

## 更新到新版本

```bash
cd aeron-analysis
cd aeron && git fetch origin && git checkout <new-commit> && cd ..
git add aeron
# 对比新旧版本差异，更新文档
git commit -m "Update aeron submodule to <new-commit>"
```

> **注意**：`docs/index.md` 是 GitHub Pages 的入口页面，Jekyll 构建时会将其生成为 `index.html`。缺少此文件会导致站点根路径 404。新增文档时无需动它，但不要删除。

## 创建 GitHub Pages 检查清单

首次部署或迁移仓库时，确认以下文件齐全：

```
docs/
├── index.md          # ★ 必须：首页，Jekyll 生成 index.html
├── _config.yml       # ★ 必须：Jekyll 配置（remote_theme, url 等）
├── 01-xxx.md         # 分析文档（需要有 YAML frontmatter: title + nav_order）
├── 02-xxx.md
└── ...
```

`.github/workflows/deploy.yml` 中的 link rewrite step 需要注意：
- `sed` 的匹配模式要与文档中的链接格式一致（如 `](aeron/...`）
- submodule 路径变更时需要同步更新
- `submodules: false` 加速 checkout（只需 submodule 的 commit hash）

## 贡献

欢迎提交 PR 修正错误或补充新内容。请确保引用的行号与当前 submodule 版本一致。

## License

MIT
