# 数据湖（Data Lake）深度学习笔记

> **学习笔记系列说明**
>
> 本系列的前 5 篇聚焦**数据仓库建模方法论**：
> 1. `01-inmon.md` — Inmon 企业信息工厂（CIF）与 3NF 范式建模
> 2. `02-kimball.md` — Kimball 维度建模与总线架构
> 3. `03-medallion.md` — Medallion（Bronze/Silver/Gold）分层架构
> 4. `04-dbt.md` — dbt 与 Analytics Engineering 工程范式
> 5. `05-dagster.md` — Dagster 与数据资产（Data Asset）编排
>
> **本篇（06）起进入第二批主题「数据平台基础设施」**，计划涵盖：
> 6. **`06-data-lake.md` — 数据湖 / 湖仓一体（Data Lake / Lakehouse）（本篇）**
> 7. 离线计算（Batch / MapReduce / Spark）
> 8. 实时计算（Streaming / Flink）
> 9. OLTP（联机事务处理）
> 10. OLAP（联机分析处理）
>
> 本篇与 `03-medallion.md`（分层范式）、`04-dbt.md`（转换工程化）密切呼应：Medallion 是数据湖 / 湖仓之上的**组织范式**，本篇则讲清它所依附的**存储与表格式基础设施**。
>
> 目标读者：有一定数据工程基础、希望系统理解数据湖本质与湖仓演进的工程师。术语首次出现时保留英文原文。

---

## 0. TL;DR（快速结论）

- **数据湖（Data Lake）** 是一个**集中式仓储**，以**原始 / 原生格式**、**任意规模**存储结构化、半结构化、非结构化数据，核心哲学是 **schema-on-read（读时模式）**。[¹][²][³]
- 「Data Lake」一词由 Pentaho 前 CTO **James Dixon** 于 **2010 年左右**提出，用来对比 **Data Mart（数据集市）**：数据集市像「瓶装水」（已清洗、包装、结构化），数据湖则像「一整湖原水」（原始、可从多角度取用）。[⁴][⁵][⁶]
- 数据湖的技术骨架分四层：**对象存储（S3/HDFS/OSS）→ 文件格式（Parquet/ORC/Avro）→ 表格式（Delta Lake/Iceberg/Hudi）→ 元数据目录（Hive Metastore/Glue/Unity Catalog）**。[⁷][⁸]
- 数据湖天然不提供 **ACID、schema 约束、事务一致性**，治理缺位时会退化成 **Data Swamp（数据沼泽）**——数据存在却无人能信任、无人能找到。[⁹][¹⁰]
- **开放表格式（Open Table Format）** 在对象存储之上叠加了**事务日志 / 元数据层**，带来 **ACID / time travel / schema evolution**，从而催生了 **Lakehouse（湖仓一体）**——数据湖的低成本灵活性 + 数据仓库的可靠性与治理。[¹¹][¹²][¹³]
- 一句话：**数据湖解决「存得下、存得杂、存得便宜」，数据仓库解决「查得快、管得住、信得过」，Lakehouse 试图两者兼得。**

---

## 1. 起源：James Dixon 的「湖」与「瓶装水」隐喻

### 1.1 概念的诞生（2010）

「Data Lake」这一术语最早由 **James Dixon**（时任 Pentaho CTO，Pentaho 当时是 Hadoop 生态的数据集成与 BI 厂商）在 **2010 年左右**的博客中提出。[⁴][⁵][⁶] 他提出这个词，是为了与当时主流的 **Data Mart（数据集市）** 形成对比。

Dixon 的经典隐喻是：[⁶]

> 如果把 **Data Mart** 想象成一仓库的**瓶装水**——已经过清洗、打包、结构化以便饮用（消费）；那么 **Data Lake** 就是一片更大的**水体（湖）**，数据以更**原始（raw / native）**的状态从源头流入并填满这片湖，不同用户可以来到湖边，**取样、潜入、或抽取自己需要的那部分水**。

这个隐喻精准点出了数据湖区别于传统数仓 / 集市的两个根本特征：

1. **原始性**：数据以进入时的原生格式保存，不预先为某个特定报表建模；
2. **多用途 / 后置建模**：结构与含义在**被使用时**才施加，而不是在**写入时**强制。

### 1.2 为什么当时需要「湖」？

Dixon 批评 Data Mart 有几个结构性问题：[⁵]

- **规模受限**：集市只保留「有意思」的属性子集，抛弃了大量原始明细；
- **视角固化**：为回答已知问题而建模，一旦出现新问题，原始数据已被丢弃，无法回溯；
- **数据孤岛**：每个集市各自为政，跨集市整合困难。

Hadoop（HDFS + MapReduce）在 2010 年前后的成熟，第一次让「先把所有原始数据低成本存下来，之后再想怎么用」在工程上变得可行——这正是数据湖概念的技术土壤。[⁶]

> **历史坐标**：数据湖概念 → Hadoop 数据湖时代（HDFS 上的 Hive/Parquet） → 云对象存储数据湖（S3/ADLS 存算分离） → 开放表格式 → Lakehouse。本篇会沿这条线索展开。

---

## 2. 定义与核心特征

### 2.1 权威定义

各大云厂商与机构的定义高度一致，都强调「集中、原始、任意规模、任意结构」：

- **AWS**：数据湖是一个**集中式仓储**，允许你以**任意规模**存储所有**结构化与非结构化数据**；可以按原样存储数据（无需先结构化），并运行不同类型的分析——从仪表盘、可视化到大数据处理、实时分析与机器学习。[¹]
- **Azure（Microsoft）**：数据湖以**原始、未转换**的状态存储**所有类型**的数据，仅在**需要时**才施加转换，这种方式称为 **schema-on-read**；与之相对，数据仓库在**写入时**强制 schema（schema-on-write）。[³]
- **Red Hat**：数据湖是一种以**原生格式**存储大量、多样原始数据的仓储，让你保留数据「未加工」的视图。[²]

### 2.2 五大核心特征

**① Schema-on-Read（读时模式）——最本质的特征**

数据湖**不在写入时强制 schema**，而是在**读取 / 查询时**才解析结构。[³][¹⁴]

```
schema-on-write（数据仓库）           schema-on-read（数据湖）
   写入前必须先定义表结构                写入时不管结构，原样落盘
   ┌─────────┐  校验/拒绝               ┌─────────┐
   │  源数据  │ ──────X──▶ [严格表]      │  源数据  │ ──直接──▶ [原始文件湖]
   └─────────┘                          └─────────┘
   优点：写入即规范、查询快              读取时才套用 schema
   缺点：僵化、丢弃不合规数据           ┌──────────┐   查询时
                                        │ 原始文件  │ ──解析──▶ [DataFrame/表]
                                        └──────────┘
                                        优点：灵活、不丢数据、支持探索
                                        缺点：查询时才发现脏数据、性能与治理挑战
```

- **schema-on-write**：先定义结构，数据必须符合结构才能写入（传统 RDBMS / 数仓）。规范但僵化，不符合的数据被拒绝或丢弃。
- **schema-on-read**：先把数据原样存下，读取时再决定如何解释。灵活、保真、利于探索性分析与 ML，但把「数据质量的账」推迟到了消费端。[¹⁴]

**② 存储原始 / 多结构数据**

同一个湖里，结构化的关系表、半结构化的 JSON/日志、非结构化的文本/图像/音视频可以**并排共存**，无需先归一化。[¹][²]

**③ 存算分离（Decoupled Storage & Compute）**

数据湖构建在对象存储之上，**存储与计算独立扩展、独立计费**。可以用 Spark、Trino、Presto、Flink 等**多种引擎**访问**同一份数据**，而不像传统 MPP 数仓那样存算耦合、引擎绑定。这是数据湖相对传统数仓在成本与开放性上的关键优势。[⁷][¹¹]

**④ 低成本对象存储**

以 S3 / ADLS / GCS / OSS / HDFS 这类**廉价、近乎无限扩展**的对象存储为底座，单位存储成本远低于数仓专有存储，天然适合「先全量存下来」的策略。[¹][⁷]

**⑤ 支持多样化工作负载**

同一份数据既服务 BI 报表，又服务数据科学 / 机器学习 / 实时分析。尤其是 ML/AI 场景需要海量原始特征数据，数据湖是其天然栖息地。[¹]

> **代价提醒**：AWS 自己的 Prescriptive Guidance 明确指出——原始数据湖**不提供 ACID 语义**，当你需要在规模化下可靠地更新、管理数据时，这会成为痛点。[¹⁵] 这正是后文开放表格式与 Lakehouse 要解决的问题。

---

## 3. 数据湖 vs 数据仓库

这是数据工程面试与选型中最经典的对比。两者不是替代关系，而是**设计哲学的取舍**。

| 维度 | 数据仓库（Data Warehouse） | 数据湖（Data Lake） |
|---|---|---|
| **Schema 时机** | schema-on-write（写时模式），先建模后写入 | schema-on-read（读时模式），先存储后解释 [³][¹⁴] |
| **数据类型** | 主要结构化数据，已清洗、已建模 | 结构化 + 半结构化 + 非结构化，原始并存 [¹][²] |
| **数据状态** | 已加工（processed / curated） | 原始 / 原生（raw / native）为主 [²] |
| **存储成本** | 较高（专有存储、常存算耦合） | 低（廉价对象存储，存算分离） [⁷] |
| **灵活性** | 低——新问题常需重新建模、回补数据 | 高——原始数据全留，可随时从新角度分析 [⁵] |
| **查询性能** | 高——为分析优化，索引/物化/MPP | 原始态较弱，依赖引擎与文件格式优化 |
| **ACID / 事务** | 原生支持 | 原始湖**不支持**（需靠开放表格式补齐） [¹⁵] |
| **数据治理** | 强——schema 强约束、权限、血缘成熟 | 弱——需额外建设 catalog / 权限 / 质量，否则成沼泽 [⁹][¹⁰] |
| **典型用户** | 业务分析师、BI 报表 | 数据科学家、ML 工程师、数据工程师 [¹] |
| **典型工作负载** | BI、固定报表、结构化 SQL 分析 | 探索性分析、ML/AI、大数据处理、实时分析 [¹] |
| **建模范式** | ETL（先转换后加载） | ELT（先加载后转换） |
| **代表技术** | Redshift、Snowflake、BigQuery、Teradata | S3+Spark、HDFS+Hive、ADLS、Trino/Presto |

**一句话记忆：**

- **数据仓库**：先想清楚要问什么问题，再把数据规整成能高效回答这些问题的样子——**规范、快、可信，但僵化、贵**。
- **数据湖**：先把所有原水存下来，问题以后再说——**灵活、便宜、保真，但慢、乱、需治理**。

> 这一取舍的本质，是**「预先付出建模成本」还是「延迟到消费时付出解释成本」**。数据仓库把复杂度前置，数据湖把复杂度后置。而 **Lakehouse（第 6 节）** 试图用开放表格式在「一份数据」上同时拿到两者的好处。

---

## 4. 数据湖的典型技术栈（四层）

现代（云原生）数据湖可以拆成自底向上的四层。理解这个分层，是理解「数据湖 → Lakehouse」演进的关键。

```
┌───────────────────────────────────────────────────────────┐
│ ④ 元数据 / 目录层  Hive Metastore · AWS Glue · Unity Catalog │  ← 数据在哪、schema、权限、血缘
├───────────────────────────────────────────────────────────┤
│ ③ 表格式层        Delta Lake · Apache Iceberg · Apache Hudi │  ← 事务日志/快照 → ACID/time travel
├───────────────────────────────────────────────────────────┤
│ ② 文件格式层      Parquet · ORC · Avro（+ JSON/CSV 原始）    │  ← 列存/行存、压缩、编码
├───────────────────────────────────────────────────────────┤
│ ① 对象存储层      Amazon S3 · Azure ADLS · GCS · 阿里OSS · HDFS │  ← 廉价、无限扩展、存算分离
└───────────────────────────────────────────────────────────┘
```

### 4.1 ① 对象存储层：湖的「盆地」

- **云对象存储**：Amazon S3、Azure Data Lake Storage (ADLS Gen2)、Google Cloud Storage、阿里云 OSS。特点是廉价、近乎无限扩展、高持久性、存算分离，是现代数据湖的事实标准底座。[¹][⁷]
- **HDFS**：Hadoop 分布式文件系统，第一代（本地/自建）数据湖的底座。如今云上多被对象存储取代，但仍存在于大量自建集群。

对象存储的「扁平 key-value」特性带来一个隐患：它只是「一堆文件」，**没有「表」的概念，也没有原子的多文件提交**——这正是表格式层要解决的核心难题。

### 4.2 ② 文件格式层：数据怎样落到磁盘

| 格式 | 存储方式 | 定位 | 适用场景 |
|---|---|---|---|
| **Parquet** | 列式（columnar） | 分析型事实标准，压缩率高、只读所需列、I/O 小 | OLAP 分析、Spark/Trino 读多写少 [⁸][¹⁶] |
| **ORC** | 列式（columnar） | 源自 Hive/Hadoop 生态，压缩与谓词下推优秀 | 传统 Hive 栈；广义生态被 Parquet 胜出 [¹⁶] |
| **Avro** | 行式（row-based） | 为流式/逐条写入与 schema 演进设计，序列化快 | 事件流、Kafka 消息、写多、整行读取 [¹⁶] |

**核心权衡——列存 vs 行存：**

- **列存（Parquet/ORC）**：同列数据连续存放，分析查询「只读需要的几列」可极大减少 I/O，压缩率也更高（同列同类型）。**读多分析型首选**；从 CSV 换到 Parquet 常能显著降低查询时间与存储占用。[¹⁶]
- **行存（Avro）**：整行连续存放，适合「每次写入/读取一整条记录」的流式与事务型场景，且对 schema 演进友好。

> **经验法则**：分析层（Silver/Gold）几乎总是 **Parquet**；接入流式原始数据（Bronze 前的落地）常用 **Avro**。注意：这些都只是**文件格式**，本身**不构成「表」**——它们不知道「哪些文件属于同一张表的当前版本」。

### 4.3 ③ 表格式层：把「一堆文件」变成「一张表」

**开放表格式（Open Table Format）** 是一个**架在数据文件之上的标准化元数据层**：它通过一份**事务日志 / 快照清单（manifest）**，明确记录「哪些文件构成这张表的哪个版本」，从而把对象存储里散乱的 Parquet/ORC 文件**组织成一张具备数据库语义的表**。[¹³]

三大主流实现：

- **Delta Lake**（Databricks 开源）：在 Parquet 之上叠加一份**有序、只追加的事务日志（DeltaLog / `_delta_log`）**，用 **MVCC（多版本并发控制）** 实现 ACID；日志会定期压缩成 Parquet 以加速元数据操作。带来 ACID、schema enforcement、time travel、增量处理。[¹⁷][¹⁸][¹⁹]
- **Apache Iceberg**（Netflix 起源）：为**超大分析表**设计的开放表格式。核心能力包括**隐藏分区（hidden partitioning）**、**就地 schema 演进**（用永不复用的**字段 ID** 跟踪列，支持增删改列、改分区布局而无需重写数据）、以及基于**快照（snapshot）**的 time travel——每次写入/更新/删除/schema 变更都生成一个自包含的新快照。[²⁰][²¹][²²]
- **Apache Hudi**（Uber 起源）：以**增量处理（incremental processing）+ upsert / delete** 为核心卖点，把数据库能力（表、事务、可变性、索引、存储布局）与**增量流式处理模型**带到数据湖；提供多模索引（record-level index、bloom filter、bucket index 等）加速写入侧的更新删除。是「事务型/流式数据湖」运动的先驱。[²³][²⁴][²⁵]

> **一句话区分**：三者都提供 ACID/time travel/schema evolution；**Delta** 与 Databricks/Spark 生态最紧、日志模型最简洁；**Iceberg** 引擎中立、大表演进与多引擎互操作最强，正成为开放生态的汇聚点；**Hudi** 在**增量 upsert / 近实时写入**上历史最深。

### 4.4 ④ 元数据 / 目录层：数据的「地图」

即使有了表格式，你还需要一个**全局目录**回答「有哪些表、它们的 schema 是什么、谁能访问、字段含义是什么」：

- **Hive Metastore**：Hadoop 时代的事实标准表元数据服务，至今仍被广泛兼容。
- **AWS Glue Data Catalog / AWS Lake Formation**：云上托管的中心化目录 + 细粒度权限治理，Lake Formation 在 Glue Catalog 之上集中管理库/表的按角色访问控制。[²⁶][²⁷]
- **Unity Catalog**（Databricks）：Lakehouse 的统一治理层，管理表/文件/ML 模型/权限/血缘。

> 这一层与本系列 `04-dbt.md` 的血缘、`03-medallion.md` 的分层治理直接相关——**目录 + 质量 + 血缘** 是数据湖不退化为沼泽的关键防线（见第 5 节）。

---

## 5. Data Swamp（数据沼泽）：治理缺位的必然结局

### 5.1 什么是数据沼泽

**数据沼泽（Data Swamp）** 是数据湖**在缺乏治理时的退化形态**：数据虽然还在，但**无法被发现、无法被理解、无法被信任**。文件不断堆积，却没有清晰的血缘（lineage）、没有元数据、没有质量控制，重复数据泛滥，任何想找答案的人都无从下手。[⁹][¹⁰]

常见成因：[¹⁰]

- **缺元数据 / 缺 catalog**：不知道有哪些数据、字段是什么含义、从哪来、多新；
- **缺质量控制**：schema-on-read 把脏数据全部原样存下，无人在入湖或消费时校验；
- **缺文档与所有权**：数据无主，无人负责其准确性与时效；
- **缺访问治理**：权限、合规缺失，敏感数据风险与信任危机并存。

> **本质诊断**：数据湖的最大优点（schema-on-read、来者不拒）恰恰是它退化为沼泽的最大风险源——**灵活性的另一面是无约束**。数据湖不是「建好就完事」，治理是持续投入。

### 5.2 从沼泽中自救：治理要素

| 治理维度 | 手段 |
|---|---|
| **元数据管理** | 数据目录（Glue/Unity Catalog）、技术+业务元数据、schema 注册中心 |
| **数据发现** | 可搜索的 catalog、数据字典、字段业务语义描述（呼应本项目的 `catalog/*.yaml`） |
| **数据质量** | 入湖/分层时的质量规则校验（呼应本项目的 `quality/*.py`）、data contract（数据契约）[⁹] |
| **血缘（lineage）** | 记录数据从源到消费的流转路径，便于影响分析与排障（呼应 dbt/Dagster） |
| **访问治理** | 细粒度权限（如 AWS Lake Formation 的按角色库/表级权限）、审计 [²⁶][²⁷] |
| **分层组织** | Bronze/Silver/Gold（Medallion）用「质量渐进」的分层约定对抗混乱（见 `03-medallion.md`） |

> **与本项目呼应**：本仓库 `CLAUDE.md` 强制的「三层数据资产（model → quality → catalog 一一对应）」，正是「对抗数据沼泽」这一原则在工程上的落地——**每个接口必须有类型定义（model）、字段级质量规则（quality）、业务语义地图（catalog）**，本质就是给数据湖/数仓的每份资产强制配齐元数据、质量与文档。

---

## 6. 湖仓一体（Lakehouse）：数据湖的下一步演进

### 6.1 演进动机：两个世界的割裂

在 2020 年前后，企业数据栈普遍是「双系统」：一边是廉价灵活但不可靠的 **数据湖**（S3/HDFS 上的文件），另一边是可靠高性能但昂贵封闭的 **数据仓库**（Redshift/Snowflake）。数据要在两者之间反复 ETL 搬运，导致**数据陈旧（staleness）、双份存储成本、双份治理、以及厂商锁定（lock-in）**。[¹¹][¹²]

```
        传统双系统                              Lakehouse（湖仓一体）
 ┌──────────┐   ETL   ┌──────────┐          ┌───────────────────────────┐
 │ Data Lake│ ──────▶ │ Data WH  │          │  BI · SQL · ML · 数据科学    │  多引擎/多负载
 │ 便宜/灵活 │         │ 可靠/贵/封闭│    ==>   ├───────────────────────────┤
 │ 不可靠    │         │          │          │ 开放表格式 Delta/Iceberg/Hudi │  ACID/治理层
 └──────────┘         └──────────┘          ├───────────────────────────┤
   ML/原始              BI/报表               │  廉价对象存储 + 开放文件格式    │  一份数据
 两份数据 · 两套治理 · 数据陈旧 · 锁定          └───────────────────────────┘
```

### 6.2 Lakehouse 的定义（CIDR 2021 论文）

**2021 年**，Databricks 的 Armbrust、Ghodsi、Xin、Zaharia 在 CIDR 会议发表论文 **《Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics》**，正式提出并命名 Lakehouse 范式。[²⁸] 论文主张：**我们今天所知的数据仓库架构将在未来几年逐渐凋零，被一种新范式 Lakehouse 取代**，它将 **(i) 基于开放的直接访问格式（如 Parquet），(ii) 提供一流的机器学习与数据科学支持，(iii) 具备顶级的性能**；从而解决数据陈旧、可靠性、总拥有成本、数据锁定、用例受限等痛点。[²⁸]

Databricks 官方术语的定义：Lakehouse 是一种**新的开放数据管理架构**，它把**数据湖的灵活性、低成本、规模**与**数据仓库的数据管理能力和 ACID 事务**结合在一起。[¹²] 技术上，Lakehouse 通常构建在 **Delta Lake / Iceberg / Hudi 这类开放表格式**之上，而这些表格式又叠加在对象存储上的 **Parquet 等开放文件格式**之上。[¹³]

### 6.3 开放表格式带来的关键能力

Lakehouse 之所以成立，是因为开放表格式在「一堆文件」上补齐了传统数仓才有的能力：

- **ACID 事务**：多文件写入要么全成功要么全失败，读者永远看到一致快照。Delta 通过 append-only 事务日志 + MVCC 实现，把「一个文件夹的 Parquet」变成可靠可查询的数据库。[¹⁷][¹⁸]
- **Time Travel（时间旅行 / 数据版本）**：每次提交生成一个快照，可查询「某个历史版本/时间点」的表状态——利于审计、复现、回滚、ML 训练集回溯。[²⁰][²²]
- **Schema Evolution（模式演进）**：安全地增删改列、改分区布局。Iceberg 用**永不复用的字段 ID** 跟踪列，做到就地演进而无需重写全部数据。[²⁰][²¹]
- **Schema Enforcement（模式约束）**：写入时可强制约束，防止脏数据污染——把数据湖「读时才发现脏」的痛点部分前移到写入时。[¹⁸]
- **增量处理 / Upsert / Delete**：Hudi 尤其擅长以流式增量方式做 upsert 与删除（如 GDPR 删除），使近实时数据湖成为可能。[²³][²⁴]

> **关系澄清**：**数据湖是「存储层设施」，Lakehouse 是「在数据湖之上通过开放表格式补齐数仓能力后的架构」**。Lakehouse 不是抛弃数据湖，而是给数据湖装上「数据库的骨架」。因此有一种常见说法：**Lakehouse = Data Lake + 开放表格式（ACID/治理层）**。

---

## 7. 数据湖 / Lakehouse 与 Medallion 架构的关系

本系列 `03-medallion.md` 已详述 Medallion，这里只讲**它与本篇的接口关系**：

- **Medallion（Bronze/Silver/Gold）是「组织范式」，数据湖/Lakehouse 是「基础设施」**。Medallion 回答「湖里的数据资产按什么逻辑分层组织」，而本篇回答「这些资产存在哪、以什么格式、靠什么保证可靠」。
- Medallion 的每一层（Bronze 保真原始 → Silver 清洗规范 → Gold 服务聚合）**通常都物化为开放表格式的表**（Delta/Iceberg/Hudi），正是靠表格式的 **ACID / schema enforcement / time travel** 来保证「每跳一层，质量提升一层」是可靠、可重放、可审计的。[¹⁷]
- 换句话说：**没有开放表格式带来的可靠性，Medallion 的分层就只是「一堆按目录命名的文件夹」，随时可能退化成分了层的数据沼泽**。

```
   业务组织范式（03-medallion）        ┌─ Gold   （聚合/服务，Kimball星型可在此）
        Medallion 分层        ────▶   ├─ Silver （清洗/规范化明细）
                                      └─ Bronze （原始保真落地）
                                            │  每一层都物化为 ↓
   基础设施（06-data-lake，本篇）      开放表格式表（Delta/Iceberg/Hudi）
                                            │  文件格式 ↓
                                       Parquet/ORC/Avro
                                            │  存储 ↓
                                       对象存储 S3/ADLS/OSS
```

> 延伸阅读：转换如何工程化落地见 `04-dbt.md`；分层与转换如何编排、资产化见 `05-dagster.md`。

---

## 8. 适用场景、优缺点与选型建议

### 8.1 数据湖适合的场景

- **数据源多样、结构不定**：需要同时容纳结构化表、半结构化日志/JSON、非结构化文本/图像/音视频；[¹]
- **机器学习 / 数据科学**：ML 需要海量原始特征数据与探索性访问，数据湖是天然栖息地；[¹]
- **「先存下来，用途待定」**：无法预先确定所有分析问题，需要保留原始数据以备将来从新角度回溯；[⁵]
- **大数据处理 / 存算分离**：数据量巨大、需要多引擎（Spark/Trino/Flink）弹性计算、按需付费。

### 8.2 优缺点总结

**优点**
- 存储成本低、几乎无限扩展；[⁷]
- 灵活（schema-on-read）、保真（原始数据不丢）、支持多结构与多负载；[²][³]
- 存算分离、引擎开放、避免厂商锁定。[¹¹]

**缺点 / 风险**
- 原始湖无 ACID、无 schema 约束，可靠性弱；[¹⁵]
- 治理不到位极易退化成**数据沼泽**（发现难、信任难、合规难）；[⁹][¹⁰]
- 原始态查询性能不如数仓，脏数据问题延迟到消费端暴露；[¹⁴]
- 运维与治理（元数据、质量、权限、血缘）是**持续成本**，非一次性投入。

### 8.3 选型建议

- **主要是结构化数据 + 固定 BI 报表 + 强一致性要求** → 传统数据仓库仍然高效省心。
- **多结构数据 + ML/探索 + 大规模 + 成本敏感** → 数据湖 / Lakehouse。
- **既要湖的灵活便宜，又要数仓的可靠治理**（多数现代团队的诉求）→ **Lakehouse**：在对象存储 + 开放表格式（Delta/Iceberg/Hudi）之上，配齐 catalog / 质量 / 血缘治理，并用 Medallion 组织分层。
- 无论选哪种，**从第一天就建设元数据、质量与权限治理**——这是数据湖不沦为沼泽的唯一保险。

---

## 9. 小结

- 数据湖由 **James Dixon（2010）** 以「原水湖 vs 瓶装水集市」的隐喻提出，核心是 **schema-on-read**、以**原始格式任意规模**存储**多结构数据**，构建在**低成本对象存储**上，**存算分离**。[⁴][¹][³]
- 相对数据仓库，它用「**延迟到消费时付出解释成本**」换取灵活、便宜、保真，但代价是**无 ACID、弱治理、易成沼泽**。[¹⁵][⁹]
- 技术栈四层：**对象存储 → 文件格式（Parquet/ORC/Avro）→ 表格式（Delta/Iceberg/Hudi）→ 元数据目录**。开放表格式是把「一堆文件」变成「一张可靠的表」的关键。[⁸][¹³]
- **开放表格式**带来 **ACID / time travel / schema evolution**，催生了 **Lakehouse（CIDR 2021）**——数据湖的灵活低成本 + 数据仓库的可靠治理。[²⁸][¹²]
- **Medallion 是组织范式，数据湖/Lakehouse 是基础设施**；本项目强制的 model/quality/catalog 三层资产，正是「对抗数据沼泽」原则的工程落地。

下一篇将进入**离线计算（Batch）**，讲清数据湖之上的数据是如何被大规模批量处理的。

---

## 参考文献

1. AWS, *What is a Data Lake? — Introduction to Data Lakes and Analytics* — https://aws.amazon.com/what-is/data-lake/
2. Red Hat, *What is a data lake?* — https://www.redhat.com/en/topics/data-storage/what-is-a-data-lake
3. Microsoft Azure Architecture Center, *What Is a Data Lake?* — https://learn.microsoft.com/en-us/azure/architecture/data-guide/scenarios/data-lake
4. Wikipedia, *Data lake*（James Dixon coined the term by 2011 to contrast with data mart）— https://en.wikipedia.org/wiki/Data_lake
5. DATAVERSITY, *A Brief History of Data Lakes* — https://www.dataversity.net/brief-history-data-lakes/
6. James Dixon's Blog, *Data Lakes Revisited*（原始概念作者本人博客）— https://jamesdixon.wordpress.com/2014/09/25/data-lakes-revisited/
7. AWS, *Data Lakes on AWS* — https://aws.amazon.com/big-data/datalakes-and-analytics/datalakes/
8. AWS Whitepaper, *Storage Best Practices for Data and Analytics Applications (Building Data Lakes)* — https://docs.aws.amazon.com/whitepapers/latest/building-data-lakes/data-lake-foundation.html
9. Shashank Guda, *Why Your Data Lake Became a Swamp & How Data Contracts Can Save It* — https://shashankguda.medium.com/why-your-data-lake-became-a-swamp-how-data-contracts-can-save-it-318ac3ae49f1
10. Atlan, *Data Swamp Explained: Is It Sinking You?* — https://atlan.com/data-swamp-explained/
11. Databricks, *Data Lakes vs Data Warehouses: What Your Organization Needs to Know* — https://www.databricks.com/blog/data-lakes-vs-data-warehouses-what-your-organization-needs-know
12. Databricks Glossary, *What is a Data Lakehouse?* — https://www.databricks.com/glossary/data-lakehouse
13. MinIO, *What Is an Open Table Format? A Technical Overview* — https://www.min.io/learn/open-table-format
14. DataCamp, *What Is a Data Lake? Definition, Architecture, and Use Cases*（schema-on-read vs schema-on-write）— https://www.datacamp.com/blog/what-is-a-data-lake
15. AWS Prescriptive Guidance, *Data lakes*（原始数据湖不提供 ACID 语义）— https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/data-lakes.html
16. Towards Data Engineering, *The Data Engineer's Guide to File Formats: Parquet vs ORC vs Avro* — https://medium.com/towards-data-engineering/the-data-engineers-guide-to-file-formats-parquet-vs-orc-vs-avro-470e1d7f7643
17. Databricks Research, *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores* — https://databricks.com/research/delta-lake-high-performance-acid-table-storage-overcloud-object-stores
18. Databricks Blog, *Diving Into Delta Lake: Unpacking the Transaction Log* — https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html
19. delta-io, *Delta Lake PROTOCOL.md*（MVCC 事务实现）— https://github.com/delta-io/delta/blob/master/PROTOCOL.md
20. Apache Iceberg, *Docs — Overview* — https://iceberg.apache.org/docs/latest/
21. Apache Iceberg, *Docs — Evolution*（就地 schema/分区演进）— https://iceberg.apache.org/docs/latest/evolution/
22. Apache Iceberg, *Docs — Schemas*（字段 ID 永不复用）— https://iceberg.apache.org/docs/latest/schemas/
23. Apache Hudi, *An Open Source Data Lake Platform*（官网首页）— https://hudi.apache.org/
24. Apache Hudi Blog, *Incremental Processing on the Data Lake* — https://hudi.apache.org/blog/2020/08/18/hudi-incremental-processing-on-data-lakes
25. Apache Hudi, *Technical Specification*（数据库能力 + 增量流式处理模型 + 多模索引）— https://hudi.apache.org/learn/tech-specs
26. AWS, *Data Lake Governance — AWS Lake Formation Features* — https://aws.amazon.com/lake-formation/features/
27. AWS, *Data Lake Governance — AWS Lake Formation FAQs* — https://aws.amazon.com/lake-formation/faqs/
28. Armbrust, Ghodsi, Xin, Zaharia (2021), *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics* (CIDR) — https://www.cs.berkeley.edu/~matei/papers/2021/cidr_lakehouse.pdf
