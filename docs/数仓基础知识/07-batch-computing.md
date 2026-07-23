# 离线计算 / 批处理（Batch Processing）深度学习笔记

> **学习笔记系列 · 第二批「数据平台基础设施」（共 5 篇）**
> 6. 数据湖 / 湖仓（Data Lake / Lakehouse）
> 7. **离线计算 / 批处理（Batch Processing）—— 本篇**
> 8. 实时计算 / 流处理（Stream Processing）
> 9. OLTP —— 面向事务的在线处理
> 10. OLAP —— 面向分析的在线处理
>
> 第一批「数据仓库建模与工程」5 篇请见 `01-inmon.md`、`02-kimball.md`、`03-medallion.md`、`04-dbt.md`、`05-dagster.md`。
>
> 本篇面向已有一定数据工程基础、希望系统性理解「批处理引擎从何而来、为何这样设计、如何演进」的工程师。目标是既讲清 MapReduce → Hadoop → Hive → Spark 的技术脉络，也讲清每一代引擎的核心抽象、性能来源与工程局限，最后落到「批处理在现代数仓与调度体系中的真实角色」。

---

## 0. TL;DR（快速结论）

- **批处理（Batch Processing）** 的本质是：对一个**有界数据集（bounded dataset）** 做**周期性、一次性、高吞吐**的计算，用「较高延迟」换「较低单位成本 + 结果准确完整」。[¹⁴][¹⁶]
- 现代批处理引擎的思想源头是 Google 2004 年的 **MapReduce** 论文：把「大规模数据处理」抽象成 `map` + `reduce` 两个用户函数，框架自动负责并行化、数据分发、容错与调度。[¹]
- 开源世界据此实现了 **Hadoop 生态**：`HDFS`（分布式存储）+ `MapReduce`（计算）+ `YARN`（资源调度）三件套。[⁴][⁵][⁶]
- **Hive** 在 MapReduce 之上提供 `SQL`（HiveQL），把「写 Java MapReduce」降维成「写 SQL」，开启了 **SQL on Hadoop** 时代。[⁸][⁹]
- **Spark** 用 **RDD（弹性分布式数据集）+ DAG 执行 + 内存计算** 取代了「每一步都落盘」的 MapReduce，官方宣称内存场景可快 **100x**、磁盘场景快 **10x**。[²][¹⁰]
- Spark 的 **DataFrame + Catalyst 优化器** 进一步把「函数式 API」和「声明式 SQL」统一，让引擎能自动优化执行计划。[¹²][¹³]
- 在现代数据栈里，批处理是 **T+1 离线数仓 / ELT** 的主力，通常由 **Airflow / Dagster** 这类调度器周期性触发（见 `05-dagster.md`）。
- 批处理 vs 流处理不是「谁取代谁」，而是**有界 vs 无界、吞吐 vs 延迟**的取舍；在 **Lambda 架构** 里两者并存（下一篇 `08` 展开）。[¹⁴][¹⁷]

---

## 1. 什么是离线计算 / 批处理

### 1.1 定义与四个关键特征

> 批处理：在一个**已经完整存在**的数据集上，按调度周期成批地读取、转换、写出，等整批算完后一次性交付结果。[¹⁵][¹⁶]

它有四个相互关联的特征：

| 特征 | 含义 | 工程含义 |
|---|---|---|
| **有界数据（bounded）** | 输入是一个「已知边界」的快照，例如「昨天全天的订单」「截至今日的全量用户表」 | 作业启动时就知道要处理多少数据，可做全局排序、全局聚合、精确去重 |
| **高吞吐（high throughput）** | 一次处理 GB~PB 级数据，把启动开销摊薄到海量记录上 | 单位数据成本低，适合「算得多、算得全」 |
| **高延迟（high latency）** | 结果在整批算完后才可用，通常分钟级到小时级 | 不适合「来一条算一条」的实时场景 |
| **周期性调度（scheduled）** | 由调度器按 cron / 依赖关系周期性触发，如每日凌晨跑 T+1 | 天然契合「日报、月报、离线特征」类需求 |

### 1.2 一个直观例子

以本项目（tushare-dashboard）为例：每天收盘后，把当天所有股票的行情（`bak_daily`、`daily_basic` 等）从 Bronze 落到 Silver，再聚合成 Gold 服务层——这是**典型的 T+1 批处理**。它不需要「毫秒级实时」，但需要「把当天数据算全、算准」，并在次日开盘前产出。这正是批处理的舒适区。

### 1.3 为什么需要「框架」而不是自己写脚本

当数据从「单机能装下」增长到「一台机器装不下、算不完」时，你被迫**分布式**。而分布式会立刻引入三个硬骨头：

1. **并行化**：怎么把数据切片、分发到上百台机器？
2. **容错**：跑到一半某台机器挂了，怎么办？重算哪一部分？
3. **调度与数据本地性**：任务放到哪台机器上跑，才能尽量「计算靠近数据」而不是把 PB 数据搬来搬去？

MapReduce 的历史贡献，就是把这三件「又难又重复」的事**从用户代码里抽走、塞进框架**，让工程师只写业务逻辑。[¹] 这是理解后续所有批处理引擎的起点。

---

## 2. 历史脉络：MapReduce → Hadoop → Hive → Spark

批处理引擎的演进，本质是**一条「不断降低使用门槛、不断提升执行效率」的曲线**：

```
2004        2006              2009               2010~2014
Google      Hadoop            Hive               Spark
MapReduce   HDFS+MR+YARN      SQL on Hadoop      RDD/DAG/内存计算
论文         开源实现          用 SQL 写批处理     用内存替代反复落盘
  │            │                 │                   │
  │  "把并行/    │  "让人人都能    │  "让不会 Java     │  "让批处理
  │   容错/调度   │   跑分布式      │   的分析师也能    │   快一个
  ▼   抽象掉"     ▼   批处理"       ▼   写批处理"       ▼   数量级"
———————————————————————————————————————————————————————————▶ 门槛↓ 性能↑
```

### 2.1 MapReduce（2004，Google 论文）

Jeffrey Dean 与 Sanjay Ghemawat 在 OSDI 2004 发表 **《MapReduce: Simplified Data Processing on Large Clusters》**。论文的核心贡献不是「发明了 map/reduce」（这两个是函数式编程里的老概念），而是**证明了：只要用户把计算表达成 map + reduce，框架就能自动在成千上万台廉价机器上并行执行，并自动处理机器故障**。[¹] 论文明确指出，程序员无需分布式经验也能利用大规模集群资源。[¹]

### 2.2 Hadoop（2006 起，Apache 开源实现）

Doug Cutting 等人依据 Google 的 MapReduce 与 GFS 论文，做出了开源实现 Hadoop。到 Hadoop 2.x，它稳定为**三层架构**：[⁴][⁶]

- **HDFS（Hadoop Distributed File System）**：分布式存储。把大文件切成固定大小的 **block**（默认 128MB），每个 block 多副本（默认 3 份）分散存储；由 **NameNode** 管理元数据（文件→block→机器的映射），**DataNode** 存实际数据块。设计目标是「一次写入、多次读取」的高吞吐顺序访问，而非低延迟随机读写。[⁶]
- **MapReduce**：计算框架（Hadoop 里的具体实现）。
- **YARN（Yet Another Resource Negotiator）**：Hadoop 2.x 引入的资源调度层。YARN 的核心思想是**把「资源管理」和「作业调度/监控」拆成独立的守护进程**：一个全局的 **ResourceManager（RM）** 负责集群资源分配，每个应用有一个 **ApplicationMaster（AM）** 负责本作业的任务调度，**NodeManager** 在每台机器上管理 container。[⁴][⁵] 这一拆分让 Hadoop 从「只能跑 MapReduce」变成「能跑 MapReduce / Spark / Tez / Flink 等多种计算框架」的通用资源平台。[⁵]

```
                    Hadoop 2.x 生态三件套
   ┌───────────────────────────────────────────────┐
   │                   YARN（资源调度）               │
   │   ResourceManager ── NodeManager × N            │
   │        │                  │                     │
   │   ApplicationMaster   Container(Map/Reduce Task) │
   └───────────────────────────────────────────────┘
                          │ 计算读写
   ┌───────────────────────────────────────────────┐
   │                  HDFS（分布式存储）              │
   │   NameNode（元数据） ── DataNode × N（数据块）    │
   └───────────────────────────────────────────────┘
```

### 2.3 Hive（2009，SQL on Hadoop）

直接写 MapReduce（Java）门槛很高：一个简单的「按城市分组求和」都要写几十行样板代码。Facebook 团队为此做了 **Apache Hive**——一个建立在 Hadoop 之上的数据仓库软件，让用户用 **类 SQL 的 HiveQL** 读写、管理分布式存储上的大规模数据集。[⁸][⁹]

其关键部件：[⁷][⁹]
- **Metastore**：存储表、分区、列、类型、序列化方式等结构信息——这是 Hive 把「HDFS 上的裸文件」变成「有 schema 的表」的关键。这套 Hive Metastore 后来成为整个大数据生态的事实标准元数据服务。
- **Query Processor（编译器）**：把 HiveQL 编译成一张 **MapReduce 作业的 DAG**，再按依赖顺序提交执行。[⁷]

Hive 的历史意义：它让「不会 Java 的数据分析师」也能跑分布式批处理，**把批处理从『工程师专属』普及为『数据团队通用工具』**，也奠定了「用 SQL 描述离线数仓 ETL」的范式（这一范式今天在 dbt 里延续，见 `04-dbt.md`）。代价是：早期 Hive 每条 SQL 都被翻译成 MapReduce，继承了后者「反复落盘、延迟高」的缺点。

---

## 3. MapReduce 编程模型：原理与局限

### 3.1 map / shuffle / reduce 三阶段

MapReduce 把一次计算表达为两个用户函数：[¹]

- **map(k1, v1) → list(k2, v2)**：对每条输入记录做转换，产出一批「中间键值对」。
- **reduce(k2, list(v2)) → list(v3)**：对**同一个 key** 聚合到一起的所有值做归约。

两者之间有一个由框架自动完成、但对性能至关重要的隐藏阶段——**shuffle（洗牌）**：把所有 map 输出按 `key` 分区、排序，并通过网络搬运到对应的 reduce 节点，保证「相同 key 的所有 value 落到同一个 reducer」。

经典的 WordCount（词频统计）：

```
输入分片            Map 阶段                 Shuffle（按 key 分组+排序）      Reduce 阶段
"the cat"   ─▶  (the,1)(cat,1)      ┐
"the dog"   ─▶  (the,1)(dog,1)      ├─▶  the →[1,1,1]  ─▶ reduce ─▶ (the,3)
"a cat"     ─▶  (a,1)(cat,1)        ┘    cat →[1,1]    ─▶ reduce ─▶ (cat,2)
                                         dog →[1]      ─▶ reduce ─▶ (dog,1)
                                         a   →[1]      ─▶ reduce ─▶ (a,1)
```

### 3.2 框架替你做的事

用户只写了 `map` 和 `reduce` 两个纯函数，而框架自动完成了：[¹]
1. **输入切片（split）与并行 map**：把输入按 block 切片，尽量调度到数据所在机器（**数据本地性**）。
2. **shuffle**：分区、排序、跨网络传输中间结果。
3. **容错**：某个 task 失败就在别的机器**重新执行**（因为 map/reduce 是确定性的纯函数，重算结果一致）。
4. **落盘与备份执行**：map 输出写本地磁盘，作业中间结果写 HDFS；对「掉队者（straggler）」启动**推测执行（speculative execution）**。

### 3.3 MapReduce 的根本局限

正是这套「稳如老狗」的设计，带来了它的性能天花板：

| 局限 | 根因 | 后果 |
|---|---|---|
| **反复落盘（disk I/O 密集）** | 每个 job 的中间结果必须写磁盘/HDFS，下一个 job 再读回来 | 多阶段流水线要跑多个串联 MapReduce job，每一跳都全量落盘，I/O 成为瓶颈 |
| **迭代算法极慢** | 机器学习/图算法要迭代几十上百轮，每轮都是一次「读盘→算→写盘」 | 迭代类工作负载几乎不可用，这正是 Spark 诞生的直接动机 |
| **只有两个算子** | 一切都要硬套进 map + reduce | 复杂逻辑（join、多路聚合）被拆成多个 job，表达力差、样板代码多 |
| **高启动开销** | 每个 job 都有 JVM 启动、任务分发的固定成本 | 不适合交互式查询和小数据；延迟以分钟计 |

一句话概括：**MapReduce 用「每步都落盘」换来了极强的容错与可伸缩性，但也因此在「多阶段 / 迭代 / 交互」场景下慢得让人绝望。** Spark 的全部设计动机，几乎都可以从「如何在保住容错的前提下，不再每步落盘」这一句话推导出来。

---

## 4. Apache Spark：内存计算与 DAG 执行

### 4.1 RDD：弹性分布式数据集

Spark 的核心抽象是 Matei Zaharia 等人在 NSDI 2012 论文 **《Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing》** 中提出的 **RDD（Resilient Distributed Dataset，弹性分布式数据集）**。[¹⁰]

RDD 的定义与关键性质：[¹⁰]
- **不可变（immutable）、分区（partitioned）的记录集合**：一个 RDD 被切成多个 partition，分布在集群多台机器上。
- **只能通过粗粒度转换（coarse-grained transformation）构造**：即对整个数据集施加 `map`、`filter`、`join` 等操作，而不是对单条记录随机写。正是这个「粗粒度」限制，让容错可以极其廉价地实现。
- **基于 lineage（血缘）的容错**：RDD 不靠「复制数据」来容错，而是记录「它是如何从其他 RDD 计算出来的」这条**血缘链**。某个分区丢失时，只需沿血缘**重算这一个分区**，无需回滚整个作业，也无需昂贵的数据复制。[¹⁰]
- **可缓存在内存中（in-memory）**：RDD 可以被显式 `cache()` / `persist()` 驻留内存，供后续多次查询/多轮迭代复用，无需重新从磁盘读取或重新计算。[¹⁰] 这是它在迭代与交互场景碾压 MapReduce 的根本原因。

### 4.2 Transformation / Action 与惰性求值

RDD 上的操作分两类，这套划分是理解 Spark 执行模型的关键：

- **Transformation（转换）**：如 `map`、`filter`、`join`、`groupBy`——**惰性（lazy）**，调用时不立即计算，只是往血缘图上追加一个节点。
- **Action（动作）**：如 `count`、`collect`、`saveAsTextFile`——**触发真正的计算**。

惰性求值让 Spark 能**在真正执行前看到「整条流水线」**，从而做全局优化（例如把多个连续的窄依赖合并、尽量减少 shuffle）。[¹¹] 这与「Hive 把每条 SQL 立即翻译成独立 MapReduce job」形成鲜明对比。

### 4.3 DAG 执行：Stage 与 Shuffle 边界

当一个 action 被触发，Spark 的执行链条如下：[¹¹]

```
用户代码 (RDD/DataFrame 转换)
      │  Driver 构建
      ▼
   逻辑 DAG（血缘图）
      │  DAGScheduler 按【shuffle 边界】切分
      ▼
   Stage 1 ──shuffle──▶ Stage 2 ──shuffle──▶ Stage 3
      │  每个 Stage 拆成多个并行 Task
      ▼
   TaskScheduler 把 Task 分发到 Executor 执行
```

- **Driver** 把用户代码翻译成一张 **DAG（有向无环图）**。
- **DAGScheduler** 在 **shuffle 边界** 处把 DAG 切成若干 **Stage**：
  - **窄依赖（narrow dependency）**：父分区→子分区一对一（如 `map`、`filter`），可在同一 Stage 内**流水线（pipeline）执行**，中间结果留在内存，不落盘、不跨网络。
  - **宽依赖（wide dependency）**：需要跨分区重新分组（如 `groupByKey`、`join`），必须 **shuffle**，成为 Stage 的分界。
- **TaskScheduler** 把每个 Stage 内的 Task 分发到 **Executor**（工作进程）执行。

### 4.4 为什么 Spark 比 MapReduce 快

Spark 官网直言：内存场景可比 Hadoop MapReduce 快 **100x**，磁盘场景快 **10x**。[²] 速度来自几个叠加因素，而**不是单一「内存」魔法**：

| 加速来源 | 机制 |
|---|---|
| **内存中复用数据** | RDD 可缓存在 RAM，迭代/多次查询无需反复读盘、反复重算 [¹⁰] |
| **减少中间落盘** | 窄依赖在 Stage 内流水线执行，中间结果留内存，只在 shuffle 边界才落盘 [¹¹] |
| **DAG 全局优化 + 惰性求值** | 一次性看到整条流水线，可合并算子、最小化 shuffle，而非每步一个独立 job [¹¹] |
| **更低的任务调度开销** | 细粒度任务调度，避免 MapReduce 每 job 的重量级启动成本 |

> ⚠️ 一个常见误解：「Spark 全部在内存、MapReduce 全部在磁盘」。更准确的说法是——**Spark 尽量把中间结果留在内存、只在必要时落盘，而 MapReduce 强制每一步都落盘**。当数据放不下内存时 Spark 同样会溢写磁盘；配置不当的 Spark 作业甚至可能比等价 MapReduce 更慢、更贵。

### 4.5 Spark SQL / DataFrame / Catalyst

RDD 虽强，但它是「函数式、面向对象」的 API，引擎**看不懂业务语义**，也就难以自动优化（引擎只知道你调了个匿名函数，不知道你在做 filter 还是 projection）。为此 Spark SQL 团队在 SIGMOD 2015 论文 **《Spark SQL: Relational Data Processing in Spark》** 中引入了两样东西：[¹²]

- **DataFrame API**：带 schema 的结构化数据抽象（概念上等价于「分布式的表」），让用户用声明式方式表达「要什么」而非「怎么算」。它把关系型处理（声明式查询、优化存储）与 Spark 的函数式 API 结合起来。[¹²]
- **Catalyst 优化器**：一个**可扩展的查询优化器**，用 Scala 的模式匹配把查询表示成可被规则（rule）反复改写的**语法树（AST / 逻辑计划）**。[¹²][¹³]

Catalyst 的处理流水线大致是：[¹³]

```
SQL / DataFrame
     │
     ▼
Unresolved Logical Plan  ──(元数据/Catalog 解析)──▶  Logical Plan
     │
     ▼  规则改写：谓词下推、列裁剪、常量折叠……
Optimized Logical Plan
     │
     ▼  生成多个 Physical Plan，按代价模型选优
Selected Physical Plan
     │
     ▼  Tungsten：全阶段代码生成（whole-stage codegen）
   RDD / 字节码
```

关键收益：**无论你写 SQL 还是 DataFrame，最终都汇聚到同一个 Catalyst 优化器**，享受谓词下推（predicate pushdown）、列裁剪（column pruning）、Join 重排、代码生成等优化。[¹²][¹³] 这也是为什么现代 Spark 实践普遍推荐用 DataFrame/Spark SQL 而非裸 RDD——把优化交给引擎。

> 与 `04-dbt.md` 的联系：dbt 本质是「用 SQL 组织 ELT 转换 + 编译成引擎可执行的 SQL」，其底层引擎完全可以是 Spark SQL / Hive；dbt 负责「工程化与依赖管理」，Spark/Catalyst 负责「把 SQL 高效执行」。

---

## 5. 批处理在数据仓库中的角色

### 5.1 T+1 离线数仓与 ELT

批处理是**离线数仓（offline / T+1 data warehouse）** 的引擎底座。典型模式：

- **T+1**：今天产生的数据，明天（T+1）才在数仓里可查。凌晨调度批作业，把昨日增量抽取、清洗、建模、聚合，赶在业务上班前产出报表。
- **ELT 而非 ETL**：现代湖仓倾向先把原始数据**加载（Load）** 进湖/仓，再用引擎在库内**转换（Transform）**。这正是 Medallion 架构 Bronze→Silver→Gold 的多跳过程（见 `03-medallion.md`），每一跳都是一批批处理作业。

### 5.2 与调度器（Airflow / Dagster）配合

批作业「什么时候跑、按什么依赖顺序跑、跑失败怎么重试」由**调度器/编排器**负责——批处理引擎（Spark/Hive）负责「算」，调度器负责「何时算、算什么、算完没」：

- **Airflow**：以 **task（任务）** 为中心的调度，用 DAG 描述任务依赖，按 cron 触发。
- **Dagster**：以 **asset（数据资产）** 为中心（见 `05-dagster.md`），把「产出的数据表」作为一等公民，天然携带血缘与物化即校验。

以本项目为例：Dagster 定义 Bronze operator + 动态分区触发 dbt/Spark 转换，每天调度一次——这就是「调度器 + 批处理引擎」的标准协作形态（见 `backend/orchestration/defs/` 与 `05-dagster.md`）。

### 5.3 批处理层的数据质量

因为批处理面对的是**有界、完整**的数据集，它能做流处理难以做到的**全局质量检查**：全表去重、跨分区一致性校验、与历史快照比对。本项目的 `backend/app/quality/*.py` 质量规则、Dagster Asset Check（`quality_bridge.py`）就跑在批作业物化之后——这是批处理「算得准、算得全」优势的直接体现。

---

## 6. 批处理 vs 流处理（实时计算）

两者的根本分野是**数据的形态（有界 vs 无界）** 与 **优化目标（吞吐 vs 延迟）**：[¹⁴][¹⁵][¹⁶]

| 维度 | 批处理 / 离线计算（Batch） | 流处理 / 实时计算（Stream） |
|---|---|---|
| **数据形态** | 有界（bounded）：一个完整快照 [¹⁵][¹⁶] | 无界（unbounded）：持续到来的事件流 [¹⁵][¹⁶] |
| **触发方式** | 周期性调度（cron / 依赖） | 事件驱动，持续运行 24/7 [¹⁴] |
| **延迟** | 高：分钟~小时级，整批算完才出结果 [¹⁶] | 低：毫秒~秒级，逐条/微批处理 [¹⁴][¹⁶] |
| **吞吐** | 高：启动成本摊薄到海量记录 [¹⁶] | 单位吞吐通常较低，重在及时 |
| **结果特性** | 精确、完整（可全局排序/去重） | 常为近似/增量，需处理乱序、迟到数据 [¹⁷] |
| **基础设施成本** | 「按需唤醒」，跑完即释放，成本低 [¹⁴] | 常驻消费者 + 消息队列 + 状态存储，7×24 成本高 [¹⁴] |
| **典型引擎** | MapReduce、Hive、Spark（batch） | Flink、Spark Structured Streaming、Kafka Streams |
| **典型场景** | T+1 报表、离线数仓、离线特征、月结 | 实时风控、监控告警、实时大盘、在线特征 |

选型直觉：**「结果能等、要算全算准、数据量大」选批处理；「结果要立刻、可容忍近似」选流处理。** 注意「流」不总是「更好」——延迟是有成本的，为不需要实时的场景上流式基础设施是常见的过度工程。[¹⁴]

> 值得一提的是「批流一体（unified batch & streaming）」趋势：把批看作「流的一个有界特例」，用同一套 API 同时表达两者（Spark Structured Streaming、Apache Flink 是代表）。这将在下一篇 `08-实时计算` 展开。

---

## 7. Lambda 架构中的批处理层

**Lambda 架构** 由 Nathan Marz（Apache Storm 作者）于 2011 年前后在 Twitter 提出，核心洞察是：**没有单一系统能同时最优地兼顾「结果正确/完整」与「低延迟」，那就用两条路并行，再合并**。[¹⁷] 它分三层：[¹⁷]

```
                       ┌──────────────────────────────┐
   不可变主数据集 ─────▶│ Batch Layer（批处理层）        │
   (append-only,       │  周期性重算全量 → batch view   │──┐
    如事件日志/数据湖)   │  准确、完整、高延迟             │  │
        │              └──────────────────────────────┘  │   ┌───────────────┐
        │                                                 ├──▶│ Serving Layer  │──▶ 查询
        │              ┌──────────────────────────────┐  │   │ 合并两个 view   │
        └─────────────▶│ Speed Layer（速度层）          │──┘   └───────────────┘
                       │  只处理最近增量 → realtime view │
                       │  近似、低延迟，补批处理的"时间差" │
                       └──────────────────────────────┘
```

- **Batch Layer（批处理层）**：在**不可变、只追加**的主数据集上**周期性重算全量**，产出准确、完整的 **batch view**。它是「真相之源」——即使速度层算错了，下一次批处理全量重算会把结果**自愈**修正。[¹⁷] 这正是批处理「准确、可重放、可回溯」价值的集中体现。
- **Speed Layer（速度层）**：只处理批处理「还没覆盖到」的最新增量，产出低延迟但近似的 realtime view，填补批处理的时间差。[¹⁷]
- **Serving Layer（服务层）**：合并两个 view 响应查询。[¹⁷]

Lambda 的批处理层与流处理层各司其职，但也因「**同一套逻辑要用批和流两套代码维护**」而饱受批评，由此催生了简化的 **Kappa 架构**（只用流、把重算也当成重放流）——这些也留待 `08` 篇讨论。

---

## 8. 适用场景与优缺点

### 8.1 适用场景
- **T+1 / T+N 离线数仓与报表**：日报、周报、月结、财务对账。
- **大规模 ETL / ELT**：全量清洗、建模、聚合，Medallion 多跳转换。
- **离线特征工程与模型训练**：需要全量历史、可重复、可回溯。
- **需要全局精确的计算**：全表去重、全局排序、跨分区一致性核对。
- **回填与重算（backfill）**：数据/逻辑修正后对历史分区批量重算。

### 8.2 优点
- **高吞吐、低单位成本**：启动开销摊薄到海量记录，「按需唤醒、跑完释放」。[¹⁴]
- **结果准确完整**：面对有界数据可做全局操作，天然支持精确去重/排序。
- **强容错、可重放**：作业幂等、可对历史分区重复执行得到一致结果（MapReduce 重算、Spark 血缘重算）。[¹][¹⁰]
- **成熟生态与工具链**：Hive/Spark + Airflow/Dagster + dbt 已是工业标准。

### 8.3 缺点
- **高延迟**：不适合秒级/亚秒级实时需求。
- **资源尖峰**：调度窗口内集中占用大量资源，需容量规划。
- **数据新鲜度受限**：T+1 意味着最新数据至少滞后一个周期。
- **调优门槛**：Spark 分区/内存/shuffle 配置不当，可能比 MapReduce 更慢更贵。

---

## 9. 一页速查（Cheat Sheet）

- **一句话**：批处理 = 在**有界数据**上做**周期性、高吞吐、高延迟**的计算，用延迟换准确与低成本。
- **技术脉络**：MapReduce（抽象并行/容错）→ Hadoop（HDFS+MR+YARN 开源实现）→ Hive（SQL on Hadoop 降门槛）→ Spark（RDD+DAG+内存计算提性能）。
- **MapReduce 三阶段**：map（转换）→ shuffle（按 key 分组排序+跨网络搬运）→ reduce（归约）；短板是每步落盘、迭代慢。
- **Spark 快的原因**：内存复用 RDD + 窄依赖流水线不落盘 + DAG 惰性全局优化 + 低调度开销；官方称最高 100x。[²]
- **Spark 结构化路线**：DataFrame + Catalyst，让 SQL / 函数式 API 汇聚到同一优化器。
- **数仓角色**：T+1 离线数仓 / ELT 主力，由 Airflow/Dagster 调度，与 dbt 协作（`04`、`05` 篇）。
- **批 vs 流**：有界 vs 无界、吞吐 vs 延迟；Lambda 架构里批处理层是「准确、可自愈的真相之源」。[¹⁷]
- **下一篇**：`08-实时计算 / 流处理`——无界数据、事件驱动、批流一体与 Kappa 架构。

---

## 参考文献

1. Jeffrey Dean, Sanjay Ghemawat, *MapReduce: Simplified Data Processing on Large Clusters*, OSDI 2004（Google Research）— https://research.google.com/archive/mapreduce.html ；会议页 — https://www.usenix.org/conference/osdi-04/mapreduce-simplified-data-processing-large-clusters
2. Apache Spark 官网（"Run programs up to 100x faster than Hadoop MapReduce in memory, or 10x faster on disk"）— https://spark.apache.org/ （历史快照 https://svn.apache.org/repos/asf/spark/site/index.html ）
3. Apache Spark Research（RDD / 内存计算研究页）— https://spark.apache.org/research.html
4. Apache Hadoop YARN 官方文档（RM/AM/NodeManager 拆分）— https://hadoop.apache.org/docs/stable2/hadoop-yarn/hadoop-yarn-site/YARN.html
5. Understanding YARN architecture（Cloudera 官方文档）— https://docs.cloudera.com/runtime/7.3.1/yarn-overview/topics/yarn-apache-yarn.html
6. HDFS Architecture / Design（Apache Hadoop 官方设计文档 PDF）— https://hadoop.apache.org/docs/r1.0.4/hdfs_design.pdf ；当前版文档 — https://hadoop.apache.org/docs/current/
7. Apache Hive Design（Metastore / Query Processor → MapReduce DAG）— http://hive.apache.org/development/desingdocs/design ；DeveloperGuide — https://hive.apache.org/community/resources/developerguide/
8. Introduction to Apache Hive（官方文档）— https://hive.apache.org/docs/latest/introduction-to-apache-hive/
9. Ashish Thusoo et al., *Hive – A Warehousing Solution Over a Map-Reduce Framework*（原始论文）— https://www.researchgate.net/publication/220538285_Hive_-_A_Warehousing_Solution_Over_a_Map-Reduce_Framework
10. Matei Zaharia et al., *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing*, NSDI 2012 — https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia ；PDF — https://www.cs.princeton.edu/courses/archive/spring13/cos598C/spark.pdf
11. Spark Architecture: Driver, Executors, DAG Scheduler, and Task Scheduler Explained — https://abstractalgorithms.hashnode.dev/spark-architecture-driver-executors-dag-scheduler
12. Michael Armbrust et al., *Spark SQL: Relational Data Processing in Spark*, SIGMOD 2015 — https://amplab.cs.berkeley.edu/publication/spark-sql-relational-data-processing-in-spark/ ；PDF — http://people.eecs.berkeley.edu/~alig/papers/sparksql.pdf
13. Deep Dive into Spark SQL's Catalyst Optimizer（Databricks 官方博客）— https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html
14. Batch vs Streaming Processing: Key Differences（Kestra，延迟有成本 / 基础设施对比）— https://kestra.io/resources/data/batch-vs-streaming-processing
15. What are the key differences between batch and stream processing architectures?（Milvus，bounded vs unbounded）— https://blog.milvus.io/ai-quick-reference/what-are-the-key-differences-between-batch-and-stream-processing-architectures
16. Batch vs Stream Processing（AlgoMaster，吞吐/延迟/数据形态对照）— https://algomaster.io/learn/system-design/batch-vs-stream-processing
17. Lambda architecture（Wikipedia，batch/speed/serving 三层 + Nathan Marz）— https://en.wikipedia.org/wiki/Lambda_architecture ；Flexera 解析 — https://www.flexera.com/blog/finops/lambda-architecture/

> **本项目相关文件（真实工程案例来源）**
> - `docs/course/03-medallion.md` — Bronze/Silver/Gold 多跳批处理与 ELT
> - `docs/course/04-dbt.md` — 用 SQL 组织批处理 ELT 转换
> - `docs/course/05-dagster.md` — 调度器 + 批处理引擎的资产化协作
> - `backend/orchestration/defs/` — Dagster 编排批作业（Bronze operator / 动态分区 / 调 dbt）
> - `backend/app/quality/*.py`、`backend/orchestration/defs/checks/quality_bridge.py` — 批作业物化后的全局质量校验
