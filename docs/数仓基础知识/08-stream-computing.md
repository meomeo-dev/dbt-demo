---
title: 实时计算 / 流处理（Stream Processing）：无界数据、事件时间与流批一体
series: 数据平台基础设施学习笔记
part: 3 / 5
---

# 实时计算 / 流处理（Stream Processing）：从无界数据到流批一体

> **关于本系列**
> 这是「数据平台基础设施」学习笔记的第 3 篇。本系列共 5 篇，围绕「数据在平台里如何被存储、如何被计算、如何被服务」组织：
>
> 1. **数据湖（Data Lake / Lakehouse）** —— 廉价对象存储 + 开放表格式的统一存储底座
> 2. **离线计算（Batch / Offline Computing）** —— 有界数据、周期调度、吞吐优先
> 3. **实时计算 / 流处理（Stream Processing）**（本篇）—— 无界数据、低延迟、事件驱动、连续处理
> 4. **OLTP** —— 面向事务的行式在线交易处理
> 5. **OLAP** —— 面向分析的列式在线分析处理
>
> 本篇面向已有数据工程基础、希望系统理解流处理理论与工程实践的工程师。专业术语保留英文原文，正文关键论断附引用编号 `[n]`，文末「参考文献」给出可访问的真实 URL。
>
> **一句话定位**：离线计算处理「有始有终、可以等它算完」的数据；流处理处理「永远不会结束、必须边到边算」的数据。本篇的核心，是理解在一个**无界（unbounded）、乱序（out-of-order）、可能延迟到达**的数据流上，如何权衡**正确性（correctness）、延迟（latency）、成本（cost）**这三者——这正是 Google Dataflow 论文的副标题 [1]。

---

## 1. 什么是实时 / 流计算

### 1.1 从「有界」到「无界」

理解流处理，要先建立数据集的分类。Tyler Akidau 在《Streaming 101》里给出了一组比批处理 / 流处理更精确的术语 [3][4]：

- **有界数据（bounded data）**：大小有限、有明确起止的数据集。昨天全天的成交明细、某张历史快照表，都是有界数据。批处理天然处理有界数据。
- **无界数据（unbounded data）**：理论上无限增长、永不终结的数据集。用户点击流、传感器读数、交易所逐笔行情、应用日志——它们只有开始，没有结束。

Akidau 的关键观点是：**「批（batch）」和「流（streaming）」描述的其实是执行引擎的特性，不该用来描述数据本身**。无界数据既可以用批处理引擎反复切片处理，也可以用流处理引擎连续处理 [3]。真正区分流处理的，不是「数据快」，而是它被设计用来处理**无界数据**这一事实。

### 1.2 流处理的四个本质特征

| 特征 | 含义 | 与离线计算的对比 |
|---|---|---|
| **低延迟（low latency）** | 从事件产生到结果产出，以毫秒~秒计 | 离线以小时~天计 |
| **无界数据（unbounded）** | 处理永不结束的数据流 | 离线处理有始有终的数据分区 |
| **事件驱动（event-driven）** | 数据到达即触发计算，而非到点调度 | 离线由 cron / 调度器周期触发 |
| **连续处理（continuous）** | 作业 7×24 长期运行，状态持续累积 | 离线作业跑完即退出 |

一个直观对照：离线计算是「每天凌晨把昨天的账算一遍」；流处理是「每一笔交易进来就立刻更新余额、风控评分、实时大盘」。前者是**拉（pull）+ 全量重算**，后者是**推（push）+ 增量更新**。

### 1.3 为什么流处理「难」

如果数据总是**准时、有序**地到达，流处理并不难——来一条算一条即可。真正的困难在于现实世界的三个残酷事实 [1]：

1. **数据是乱序的（out-of-order）**：网络抖动、客户端缓存、移动端离线补传，使得「9:00 产生的事件」可能比「9:01 产生的事件」更晚到达系统。
2. **数据会延迟（late data）**：一条事件可能延迟几秒、几分钟甚至几小时才到。
3. **无界意味着你永远不知道「算完了没有」**：批处理有天然的「读到文件末尾」作为完成信号；流处理没有终点，你必须**自己定义「什么时候一个时间窗口的数据算收齐了」**。

Google Dataflow 论文正是把问题精确地定义为：在**大规模、无界、乱序**的数据处理中，如何平衡**正确性、延迟、成本** [1]。后面的所有概念（事件时间、窗口、水位线、触发器）都是为解决这三个事实而生的工具。

---

## 2. 核心概念

### 2.1 事件时间 vs 处理时间（Event Time vs Processing Time）

这是流处理里**最重要、最容易搞错**的一对概念 [3]。

- **事件时间（event time）**：事件**实际发生**的时间。例如一笔成交在交易所撮合成功的那一刻，记录在数据里的时间戳。
- **处理时间（processing time）**：事件**被流处理系统观测 / 处理**的时间，即系统的墙上时钟（wall clock）。

Akidau 强调：**这两者之间存在无法避免、且不断变化的偏差（skew）** [3]。由于网络延迟、生产端批量发送、broker 复制延迟、消费端调度等原因，处理时间总是晚于事件时间，且这个延迟量本身是波动的 [8]。

```
真实世界：  事件A(9:00:01)  事件B(9:00:02)  事件C(9:00:03)
                 │              │              │
             网络抖动/乱序/延迟  ↓
系统观测：  ...... C(9:00:05 到) A(9:00:06 到) B(9:00:09 到) ......
            ↑ 处理时间顺序 ≠ 事件时间顺序
```

**为什么必须用事件时间？** 因为业务的正确性定义在事件时间上。「统计 9:00~9:01 这一分钟的成交量」，指的是**在这一分钟内发生**的成交，而不是「这一分钟内恰好到达系统」的成交。如果用处理时间开窗，一旦上游延迟或重放，结果就会错乱且不可复现。用事件时间，结果才是确定的、可重放的。

代价是：用事件时间开窗，系统必须处理乱序和延迟，也就必须引入**水位线（watermark）**来判断「一个事件时间窗口大概收齐了没有」。

### 2.2 窗口（Windowing）

无界数据不能整体聚合（永远算不完），必须切成有限的「桶」来计算。**把无界流沿时间（或其他维度）切成有限块的操作，就是开窗（windowing）** [16][20]。主流三类时间窗口 [16][17]：

```
滚动窗口 Tumbling（固定大小、不重叠）：
 |--w1--|--w2--|--w3--|          每分钟成交额

滑动窗口 Sliding/Hopping（固定大小、可重叠）：
 |----w1----|
      |----w2----|               每10秒统计过去1分钟均价
           |----w3----|

会话窗口 Session（由活动间隙 gap 划分，大小不固定）：
 |--w1--|      gap      |--w2--|  用户一次会话内的行为
```

| 窗口类型 | 定义 | 是否重叠 | 典型场景 |
|---|---|---|---|
| **Tumbling（滚动）** | 固定长度、首尾相接、不重叠 | 否 | 每分钟 K 线、每小时 PV |
| **Sliding / Hopping（滑动）** | 固定长度 + 固定滑动步长，可重叠 | 是 | 移动平均、过去 5 分钟风控计数 |
| **Session（会话）** | 由「不活动间隙（gap）」动态切分，长度不定 | 否 | 用户会话、一段连续交易行为 |

Flink SQL 还提供了 `CUMULATE`（累积窗口）等变体，本质都是「如何把流切成可聚合的有限集合」的不同策略 [18]。

### 2.3 水位线（Watermark）

水位线是流处理里**最精妙**的机制，用来回答那个根本问题：「事件时间窗口的数据，收齐了没有？」

Flink 官方 API 文档的定义最精确 [11]：

> "A watermark signifies that no events with a timestamp smaller or equal to the watermark's time will occur after the watermark." [11]

翻译：**水位线 W(t) 是数据流中的一个进度标记，它断言「事件时间 ≤ t 的数据都已经到齐了，之后不会再来」**。VLDB 关于 watermark 的专门论文把它定义为「用于推理无限流中时间完整性（temporal completeness）的关键工具」 [6]。

它的工作方式：

```
事件时间轴 →  ...  [窗口 9:00~9:01 ]  ...
数据流中夹带水位线：  ← W(9:01) 到达
                     此刻系统认为 9:01 之前的数据已收齐
                     → 触发 [9:00~9:01] 窗口计算并输出结果
```

关键的**权衡（trade-off）**在于水位线推进的「保守程度」 [8]：

- 水位线**推进太快**（乐观）：窗口早早关闭、延迟低，但晚到的数据被判为「迟到」而漏算，牺牲**正确性**。
- 水位线**推进太慢**（保守，允许更大乱序容忍度）：几乎不漏数据、正确性高，但每个窗口都要等更久才输出，牺牲**延迟**。

这正是 Dataflow 论文所说「正确性 vs 延迟」权衡在工程上的具体体现 [1][6]。实践中通常用「观测到的最大事件时间 − 允许的最大乱序时间（bounded out-of-orderness）」来生成水位线。

### 2.4 乱序与迟到数据（Out-of-Order & Late Data）

当水位线已经越过某个窗口、窗口已经触发之后，才姗姗来迟的数据，就是**迟到数据（late data）**。处理策略有三档：

1. **丢弃（drop）**：默认行为，简单但丢失正确性。
2. **允许延迟（allowed lateness）**：窗口关闭后再多保留一段时间的状态，迟到数据到达时**重新触发**窗口、更新结果。Flink 的 `allowedLateness` 即此。
3. **侧输出（side output / dead-letter）**：把迟到数据单独引流到旁路，交由离线补算或人工处理。

这里体现了 Dataflow Model 的一个深刻思想：**结果不必是一次性的**。一个窗口可以先输出一个「早期近似结果」，随着迟到数据到达再输出「修正结果」——由**触发器（trigger）**和**累积模式（accumulation mode）**控制，下一节详述。

### 2.5 状态管理（State Management）

流处理作业几乎都是**有状态的（stateful）**：算「过去 5 分钟每只股票的成交额」，就必须在内存 / 磁盘里维护每只股票的累加器。Flink 官方将自己定位为「面向数据流的有状态计算（Stateful Computations over Data Streams）」引擎 [9]，可见状态是流处理的一等公民。

- **状态（state）**：算子在处理过程中累积、跨事件保留的数据（窗口聚合值、去重集合、机器学习特征、join 缓存等）。
- Flink 用 **RocksDB 等状态后端（state backend）** 管理可超出内存的大规模状态 [10]。
- 状态必须能随作业**容错恢复**——这就引出了 checkpoint。

### 2.6 Checkpoint 与 Exactly-Once 语义

**投递语义（delivery semantics）** 三档 [12]：

| 语义 | 含义 | 后果 |
|---|---|---|
| **at-most-once** | 最多一次，可能丢 | 简单、无重复，但会丢数据 |
| **at-least-once** | 至少一次，可能重 | 不丢数据，但可能重复计算 |
| **exactly-once** | 恰好一次 | 既不丢也不重，结果完全正确 |

注意：**exactly-once 通常指「effectively-once」——即对状态和最终结果的影响恰好一次，而不是物理上每条消息只被传输一次** [10]。

**Flink 的实现：checkpoint + 分布式快照**。Flink 定期向数据流注入 **barrier（栅栏）**，基于 Chandy-Lamport 算法对所有算子的状态做一致性**分布式快照（distributed snapshot）**并持久化。作业失败时，从最近一次成功的 checkpoint 恢复状态、并把数据源 offset 回退到对应位置重放 [1][10]。配合支持事务或幂等写入的 sink（两阶段提交），实现端到端 exactly-once [10]。

```
数据流 → [算子A] → [算子B] → [算子C] → Sink
            ↑         ↑         ↑
         barrier 流经每个算子，触发本地状态快照
         所有算子快照汇合 = 一次全局一致的 checkpoint
         失败时：恢复各算子状态 + source offset 回放
```

**Kafka 的实现：幂等 producer + 事务**。Kafka 通过**幂等 producer**（给每条消息附带序列号，broker 去重）+ **事务（transactions）**（跨多分区原子写入 + 原子提交 consumer offset）实现 exactly-once [2]。Kafka Streams 只需一个配置 `processing.guarantee=exactly_once_v2` 即可开启：它把「读取 → 更新状态 → 写状态 backing topic → 写输出 → 提交 offset」这一组动作打包进**同一个事务**，要么全部成功、要么全部回滚 [26]。

---

## 3. Google Dataflow Model：流处理的理论基石

2015 年 Google 在 VLDB 发表的《The Dataflow Model》（全名很长：*A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing*）[1] 是现代流处理的理论奠基之作。它的贡献不是又一个引擎，而是提供了一套**统一的思维框架**，后来直接演化成 Apache Beam 编程模型 [5]，并深刻影响了 Flink、Spark 等所有主流引擎。

### 3.1 核心洞察：批 / 流不是对立的

论文最重要的论断：**「batch」和「streaming」的传统割裂是一个历史包袱**。一旦把「正确性、延迟、成本」当作可调节的旋钮，批处理就只是流处理的一个特例（延迟旋钮拧到最大、等所有数据到齐再一次性输出）。这为后来的**流批一体**奠定了理论基础。

### 3.2 四个问题（What / Where / When / How）

Dataflow Model 把任何数据处理都拆解为四个正交问题 [5][15]：

| 问题 | 英文 | 回答的是 | 对应机制 |
|---|---|---|---|
| **算什么？** | **What** results are being computed? | 计算逻辑（求和、计数、join、训练） | Transformation（`ParDo` / `Combine`） |
| **按什么切分？** | **Where** in event time? | 在事件时间的哪些区间上聚合 | **Windowing**（窗口） |
| **何时输出？** | **When** in processing time? | 在处理时间的哪一刻实际产出结果 | **Watermark + Trigger（触发器）** |
| **如何修正？** | **How** do refinements relate? | 多次输出之间如何累积 / 覆盖 | **Accumulation mode（累积模式）** |

这个框架的威力在于**正交解耦**：你可以独立地改变「在哪切窗口」（Where）而不动「算什么」（What），也可以独立地调整「何时触发、迟到数据怎么修正」（When/How）而不动前两者。

- **When（触发器 Trigger）**：水位线越过窗口末端是最常见的触发条件，但也可以「每处理 N 条触发一次早期结果」「窗口关闭后每来一条迟到数据再触发一次」。
- **How（累积模式）**：多次触发的结果之间是 **discarding（丢弃前值，各次独立）**、**accumulating（累积，后值覆盖前值的完整修正）**，还是 **accumulating & retracting（累积并撤回，先发一条撤销再发新值）**。

正是这套 What/Where/When/How，把「早期近似 + 逐步修正」这种流处理特有的能力，变成了可组合、可推理的编程模型 [1][5]。

---

## 4. 主流引擎对比

### 4.1 Apache Kafka：不是流处理引擎，是流的「地基」

首先要澄清一个常见混淆：**Kafka 本身是一个分布式的、持久化的、可重放的 commit log（提交日志）/ 消息系统，不是流计算引擎**。它解决的是「数据流如何被可靠地传输、持久化、重放」，是几乎所有流处理架构的数据底座 [12]。

- 消息一旦被写入并 commit 到日志，只要还有一个持有该分区副本的 broker 存活，就不会丢失 [12]。
- 日志**可重放（replayable）**：消费者可以从任意 offset 重新读取历史——这是 Kappa 架构成立的关键前提（见第 7 节）。

在 Kafka 之上做流计算，有两条路：**Kafka Streams**（Kafka 自带的客户端库），或外接 **Flink / Spark**。

### 4.2 Kafka Streams：嵌入式流处理库

Kafka Streams 是一个**轻量级客户端库**，把处理逻辑表达为「read-process-write（读—处理—写）」循环，所有状态更新和结果输出都由 Kafka 的事务能力保证一致性 [27]。它不需要独立集群——应用就是普通 Java 进程，随应用一起部署和扩缩容。适合「服务内嵌的、以 Kafka 为唯一数据总线」的流处理场景。

### 4.3 Apache Flink：真流处理（true streaming）的标杆

Flink 是**原生流处理引擎**——它把一切都当作流，「逐条事件（event-at-a-time）」地处理，而非攒批 [9]。核心能力：

- 原生的**事件时间 + 水位线**语义 [10][13]；
- 强大的**有状态计算** + RocksDB 状态后端 [9][10]；
- 基于分布式快照的 **exactly-once**，可在数千节点上 7×24 运行并保证结果正确 [10][14]；
- 通过 **Dynamic Table（动态表）** 把流与 SQL 统一：一个持续变化的表就是一个流，反之亦然 [21]。

Flink 的定位就是官网那句话：**"Stateful Computations over Data Streams"** [9]。它是低延迟、复杂事件时间语义场景（金融风控、实时特征）的首选。

### 4.4 Spark Structured Streaming：micro-batch 与 continuous

Spark Structured Streaming 的核心理念是「**把流当作一张不断追加行的无界表（unbounded table）**」，你用写批处理一样的 DataFrame/SQL API 写流处理。执行上有两种模式 [22]：

- **micro-batch（微批，默认）**：把流切成一连串小批次作业执行，端到端延迟约 **100ms 量级**，并提供 exactly-once [22]。本质是「用批模拟流」。
- **continuous processing（连续处理，实验性）**：真正逐条处理，可把延迟压到 **~1ms**，但语义保证较弱（at-least-once）[22]。

优势是与 Spark 批处理生态**完全统一**（同一套 API、同一个引擎），适合已有 Spark 栈、对亚秒级延迟不苛刻的团队。

### 4.5 四引擎横向对比

| 维度 | Apache Kafka | Kafka Streams | Apache Flink | Spark Structured Streaming |
|---|---|---|---|---|
| 本质 | 分布式 commit log / 消息系统 | 嵌入式流处理**库** | 独立流处理**引擎** | Spark 之上的流模块 |
| 处理模型 | 传输 / 存储，不做计算 | event-at-a-time | **true streaming**（逐条） | **micro-batch**（默认）/ continuous |
| 部署形态 | 独立集群 | 随应用进程部署，无独立集群 | 独立集群（JobManager/TaskManager） | 复用 Spark 集群 |
| 典型延迟 | —（传输） | 毫秒级 | **亚毫秒~毫秒** | ~100ms（micro-batch）/ ~1ms（continuous） |
| 事件时间 + 水位线 | 无 | 支持 | **原生、最完善** | 支持 |
| exactly-once | 幂等 producer + 事务 [2] | `exactly_once_v2` 一键开启 [26] | checkpoint + 分布式快照 [10] | checkpoint + WAL（micro-batch）[22] |
| 状态管理 | 无 | 本地 state store + backing topic | RocksDB 大规模状态 [10] | 状态存于 checkpoint |
| SQL 统一 | 无（KSQL 另计） | KStream/KTable | Dynamic Table [21] | 无界表（DataFrame/SQL） |
| 最佳场景 | 数据总线 / 事件日志 | Kafka 内嵌轻量流处理 | 低延迟、复杂事件时间、大状态 | 已有 Spark 栈、流批统一 ETL |

---

## 5. 流处理 vs 批处理（离线计算）的边界

这一节与本系列第 2 篇「离线计算」直接呼应。二者不是「谁取代谁」，而是**用不同的旋钮设置去权衡正确性、延迟、成本** [1]。

| 维度 | 批处理 / 离线计算 | 流处理 / 实时计算 |
|---|---|---|
| 处理的数据 | **有界**（bounded），有明确起止 | **无界**（unbounded），永不终结 |
| 触发方式 | 周期调度（cron / 编排器） | 事件驱动，数据到达即算 |
| 延迟 | 分钟~小时~天 | 毫秒~秒 |
| 吞吐 vs 延迟 | **吞吐优先** | **延迟优先** |
| 数据完整性 | 天然：读到分区末尾即「齐了」 | 需靠**水位线**推断「大概齐了」 |
| 计算形态 | 全量重算，无状态或状态随作业结束 | 增量更新，**长期累积状态** |
| 结果 | 一次性、确定 | 可先近似、后修正（trigger + 累积模式） |
| 重算 / 纠错 | 重跑作业即可 | 依赖状态回滚 + 源重放（checkpoint / offset） |
| 复杂度 | 相对低 | 高（乱序、迟到、状态、容错都要处理） |
| 典型引擎 | Spark（batch）、MapReduce、Hive | Flink、Kafka Streams、Spark SS |

**关键结论**（Dataflow 论文 [1] 与 Flink 的 batch execution mode 文档 [24] 都印证）：**批处理是流处理的一个特例**——当你把「等到所有数据到齐、只在末尾触发一次、结果不再修正」作为参数设置时，一个流处理引擎就退化成了批处理引擎。Flink 的 DataStream API 甚至可以在同一份代码上切换 `BATCH` / `STREAMING` 执行模式 [24]。这个洞察，正是下面「流批一体」的理论根据。

---

## 6. Lambda 架构 vs Kappa 架构

如何在一套系统里同时满足「低延迟」和「高正确性 / 可重算」？历史上有两条著名路线。

### 6.1 Lambda 架构：批流双写

Lambda 架构（Nathan Marz 提出）用**两条并行的处理链路** [28]：

```
                    ┌─→ Batch Layer（批处理层）──→ Batch View ─┐
数据源 → 不可变日志 ─┤                                          ├→ Serving Layer → 查询
                    └─→ Speed Layer（速度层，流）→ Realtime View ┘
```

- **Batch Layer**：对全量历史数据周期性重算，产出准确、完整的结果（高正确性，高延迟）。
- **Speed Layer**：用流处理对最新数据做低延迟的近似计算，弥补批处理的滞后。
- **Serving Layer**：合并两者，对外提供查询。

**痛点**：同一套业务逻辑要在批和流两个技术栈里**各实现一遍并保持一致**，长期维护成本极高，两边逻辑漂移会导致结果对不上。

### 6.2 Kappa 架构：只留一条流

2014 年，Kafka 之父 **Jay Kreps** 发表《Questioning the Lambda Architecture》[28]，提出用**一条流处理链路搞定一切**，即 Kappa 架构。

核心洞察：**如果你的日志系统（如 Kafka）足够可靠、且可重放（replayable），那么「重算历史」就不需要单独的批处理层——只要从日志头部重新回放，用同一套流处理代码再跑一遍即可** [28]。

```
数据源 → 可重放的日志（Kafka）→ Stream Processing → Serving
                    ↑
         需要重算/改逻辑时：从头 replay 一遍（可起新作业并行重算）
```

- **优点**：单一代码库、单一技术栈，无逻辑漂移，运维简单。
- **前提**：日志必须能长期保留并可重放；流处理引擎必须足够强（正确性、状态、exactly-once）——而这正是 Flink / Kafka 成熟后才具备的条件。

### 6.3 对比

| 维度 | Lambda | Kappa |
|---|---|---|
| 处理链路 | 批 + 流 **两条** | 流 **一条** |
| 代码库 | 两套（易漂移） | 一套 |
| 重算历史 | 靠 batch layer | 从日志 **replay** |
| 运维复杂度 | 高 | 低 |
| 前提条件 | 无特殊要求 | 日志可长期保留 + 可重放 |
| 提出者 / 时间 | Nathan Marz | Jay Kreps, 2014 [28] |

随着 Flink / Kafka 生态成熟，业界重心明显向 Kappa（及其变体）迁移 [28][28]，但 Lambda 在「需要深度历史重算 + 亚秒延迟」的少数场景仍有价值。

---

## 7. 流批一体（Unified Batch & Streaming）

Kappa 架构解决了「架构层面用一条链路」，而**流批一体**要解决的是更底层的问题：**用同一套 API、同一个引擎，既能处理有界数据（批），又能处理无界数据（流）**。这正是 Dataflow Model「批是流的特例」洞察的工程落地 [1]。

- **Apache Beam**：Dataflow Model 的开源实现，一套代码可跑在 Flink / Spark / Dataflow 等多种 runner 上，天然流批统一 [5]。
- **Apache Flink**：DataStream API 支持 `BATCH` / `STREAMING` 两种执行模式，同一份代码处理有界流时自动用批优化（如排序代替持续状态）[24]；上层用 **Dynamic Table** 把「流」和「表」在语义上统一——**一个持续更新的表就是一个流，一个流的物化就是一张表**（流表二象性）[21]。
- **Spark**：Structured Streaming 让流处理复用批处理的 DataFrame/SQL API，「无界表」抽象把批和流表达为同一套算子 [22]。

流批一体的价值：**一次开发、两处运行**——历史回填（backfill）用批模式跑，实时增量用流模式跑，业务逻辑只写一遍，彻底消除 Lambda 架构的双栈维护成本。

---

## 8. 适用场景与优缺点

### 8.1 典型场景

| 场景 | 为什么需要流处理 | 关键技术点 |
|---|---|---|
| **实时风控 / 反欺诈** | 必须在交易清算前的**亚秒级**内拦截可疑交易 [23][27] | 低延迟、有状态（滑动窗口计数）、CEP |
| **实时监控 / 告警** | 系统指标、日志异常需秒级发现 | 滚动窗口聚合、阈值触发 |
| **实时推荐 / 特征** | 用户行为即时反映到推荐结果 | 有状态特征计算、事件时间 join |
| **金融实时行情** | 逐笔行情聚合成秒级 / 分钟级 K 线、实时指标 | 事件时间窗口、水位线、乱序处理 |
| **IoT / 传感器** | 海量设备数据连续接入与实时分析 | 无界流、会话窗口 |

以**实时风控**为例：把「同一账户过去 5 分钟的交易笔数 / 金额」维护为滑动窗口状态，每来一笔交易即更新并比对规则，异常立即拦截——用离线批处理（次日跑批）根本来不及 [23][25]。

### 8.2 优点

- **低延迟**：秒级甚至毫秒级洞察，支撑实时决策。
- **事件驱动**：资源随数据到达使用，避免「到点空跑」。
- **持续结果**：结果始终反映最新状态，无需等待批次窗口。
- **（流批一体下）单一代码库**：消除双栈维护成本。

### 8.3 缺点与代价

- **复杂度高**：乱序、迟到、水位线、状态、容错都要正确处理，心智负担远大于批处理。
- **正确性 vs 延迟的永恒权衡**：水位线快慢、触发策略都要按业务调，没有万能配置 [1][6]。
- **状态运维**：大规模状态的存储、快照、恢复、扩缩容都是工程难题。
- **调试 / 回归难**：7×24 长运行作业，问题复现和历史重算比批处理麻烦。
- **成本**：常驻集群 + 大状态存储，闲时也在消耗资源。

**选型建议**：延迟要求在分钟级以上、且能接受周期调度的，优先用离线批处理（更简单、更省）；只有当业务**真的需要秒级 / 毫秒级响应**时，才付出流处理的复杂度代价。很多「实时需求」其实用「小时级微批」就够了。

---

## 9. 小结

- 流处理的本质是处理**无界、乱序、可能迟到**的数据流，核心是权衡**正确性、延迟、成本** [1]。
- 用**事件时间**而非处理时间定义正确性；用**窗口**把无界流切成可聚合的有限块；用**水位线**推断「数据是否收齐」；用**触发器 + 累积模式**支持「早期近似 + 逐步修正」——这套框架来自 Google Dataflow Model 的 **What/Where/When/How** 四问 [1][5]。
- 用 **checkpoint / 事务**实现 **exactly-once**，保证长运行作业的结果正确 [10][2]。
- 引擎选型：**Kafka** 是数据总线底座；**Kafka Streams** 是嵌入式库；**Flink** 是低延迟真流处理标杆；**Spark Structured Streaming** 适合已有 Spark 栈的流批统一。
- 架构演进：**Lambda（批流双写）→ Kappa（单流 + 日志重放）→ 流批一体（一套代码两处运行）**，是「消除双栈、拥抱流优先」的一条主线 [28][1]。

> **与本系列的呼应**：第 2 篇「离线计算」讲有界数据的吞吐优先处理，本篇讲无界数据的延迟优先处理，二者在「批是流的特例」这一点上统一（第 5、7 节）。第 1 篇「数据湖」提供了两者共享的存储底座（流处理的结果常落地到 Lakehouse），第 4、5 篇的 OLTP/OLAP 则是这些计算结果最终被服务和查询的地方。

---

## 参考文献

1. The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing（VLDB 2015, Akidau et al.）— https://research.google.com/pubs/pub43864.html
2. Message Delivery Guarantees / Exactly-Once Semantics for Apache Kafka（Confluent 官方）— https://docs.confluent.io/kafka/design/delivery-semantics.html
3. Streaming 101: The world beyond batch（Tyler Akidau）/ 《Streaming Systems》第 1 章 — https://learning.oreilly.com/library/view/streaming-systems/9781491983867/ch01.html
4. Streaming Systems, Ch.1 — Terminology: What Is Streaming?（O'Reilly）— https://www.oreilly.com/library/view/streaming-systems/9781491983867/ch01.html
5. Exploring the Fundamentals of Stream Processing with the Dataflow Model and Apache Beam（What/Where/When/How）— https://www.infoq.com/articles/dataflow-apache-beam/
6. Watermarks in Stream Processing Systems: Semantics and Comparative Analysis of Apache Flink and Google Cloud Dataflow（VLDB Vol.14）— https://www.vldb.org/pvldb/vol14/p3135-begoli.pdf
7. Windowing in Apache Flink: Tumbling, Sliding, and Session Windows — https://www.conduktor.io/glossary/windowing-in-apache-flink-tumbling-sliding-and-session-windows
8. Understand time handling in Azure Stream Analytics（event time / processing time / watermark）— https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-out-of-order-and-late-events
9. Apache Flink® — Stateful Computations over Data Streams（官网）— https://flink.apache.org/
10. Stateful Stream Processing / Checkpointing（Flink 官方文档）— https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/
11. Watermark（Flink Java API 定义）— https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/api/common/eventtime/Watermark.html
12. Message Delivery Guarantees for Apache Kafka（at-most/at-least/exactly-once）— https://docs.confluent.io/kafka/design/delivery-semantics.html
13. Generating Watermarks（Flink 官方文档）— https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/
14. Exactly-Once Processing in Apache Flink（Confluent Developer）— https://developer.confluent.io/learn/streamables/exactly-once-processing-in-apache-flink/
15. Insights from paper: The Dataflow Model（四问解读）— https://hemantkgupta.medium.com/insights-from-paper-google-the-dataflow-model-a-practical-approach-to-balancing-correctness-e331688670f9
16. Windows（Flink 官方文档，窗口是处理无限流的核心）— https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/python/datastream/operators/windows/
17. Streaming Analytics（Flink learn-flink 文档，事件时间与窗口）— https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/learn-flink/streaming_analytics/
18. SQL Windowing TVFs（TUMBLE/HOP/CUMULATE/SESSION）— https://docs.confluent.io/cloud/current/flink/reference/queries/window-tvf.html
19. Time and Watermarks in Confluent Cloud for Apache Flink — https://docs.confluent.io/cloud/current/flink/concepts/timely-stream-processing.html
20. Windowing in Apache Flink（Tumbling/Sliding/Session 定义）— https://www.conduktor.io/glossary/windowing-in-apache-flink-tumbling-sliding-and-session-windows
21. Continuous Queries on Dynamic Tables（Flink 流表二象性）— https://flink.apache.org/2017/03/30/continuous-queries-on-dynamic-tables/
22. Structured Streaming Programming Guide（micro-batch vs continuous）— https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
23. Apache Kafka Stream Processing Use Cases（含风控 / 日志 / IoT）— https://ksolves.com/blog/apache-kafka/apache-kafka-stream-processing-use-cases
24. A Rundown of Batch Execution Mode in the DataStream API（Flink 流批统一执行模式）— https://flink.apache.org/2021/03/11/a-rundown-of-batch-execution-mode-in-the-datastream-api/
25. How to Build a Real-Time Fraud Detection System with Streaming SQL — https://risingwave.com/blog/real-time-fraud-detection-streaming-sql/
26. Building Systems Using Transactions in Apache Kafka® / Kafka Streams exactly_once_v2 — https://developer.confluent.io/learn/kafka-transactions-and-guarantees/
27. Rethinking Distributed Stream Processing in Apache Kafka（Kafka Streams read-process-write 白皮书）— https://www.confluent.io/resources/white-paper/distributed-stream-processing-in-kafka
28. Questioning the Lambda Architecture（Jay Kreps, O'Reilly, 2014）— https://www.oreilly.com/radar/questioning-the-lambda-architecture/

> **本系列相关篇目**
> - `docs/course/03-medallion.md` — Lakehouse 分层（流处理结果的落地存储）
> - 本系列「离线计算」篇 — 有界数据的批处理（与第 5、7 节「批是流的特例」呼应）
