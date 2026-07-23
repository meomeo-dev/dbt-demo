# 数据平台课程笔记(Course)

面向数据工程师的系统学习笔记合集。从**数仓建模方法论**到**数据平台基础设施**,再到**企业级决策方案**,由浅入深、层层递进。每篇均经联网检索权威文献(官方文档、原著、VLDB/CIDR/SIGMOD 论文)并附真实 URL 引用;`11` 系列额外经过 deep-research 的 3 票对抗式验证,显式标注置信度。

> 阅读建议:按批次顺序读;各篇开头有系列导航头与交叉引用,可跳转关联主题。

---

## 第一批 · 数仓建模方法论与工具(01–05)

数仓领域的三大建模范式与现代工具链。理解"数据该怎么组织、由谁编排"。

| 编号 | 笔记 | 主题 | 一句话 |
|---|---|---|---|
| 01 | [Inmon / CIF](01-inmon.md) | 企业信息工厂、3NF、自顶向下 | 中央 EDW 用 3NF 规范化,自顶向下建企业级单一事实源 |
| 02 | [Kimball](02-kimball.md) | 维度建模、星型模型、总线架构、SCD | 自底向上、面向 BI 消费的事实/维度建模 |
| 03 | [Medallion](03-medallion.md) | Bronze / Silver / Gold | 按数据洁净度分层的 Lakehouse 组织范式 |
| 04 | [dbt](04-dbt.md) | staging / intermediate / marts | 按转换职责模块化的 SQL 变换分层 |
| 05 | [Dagster](05-dagster.md) | Software-Defined Assets 编排 | 资产为中心的编排与质量门(控制面) |

## 第二批 · 数据平台基础设施(06–10)

存储与计算的底座原理。理解"数据存在哪、算在哪、用什么库"。

| 编号 | 笔记 | 主题 | 一句话 |
|---|---|---|---|
| 06 | [数据湖](06-data-lake.md) | schema-on-read、开放表格式、湖仓一体 | 原始多结构数据入湖,开放表格式带来 ACID/演进 |
| 07 | [离线计算](07-batch-computing.md) | MapReduce / Hadoop / Spark | 高吞吐、有界数据、周期调度的批处理 |
| 08 | [实时计算](08-stream-computing.md) | Flink / Kafka、事件时间、水位线 | 无界数据流、低延迟、事件驱动的流处理 |
| 09 | [OLTP](09-oltp.md) | ACID、隔离级别、MVCC、B+Tree、行存 | 面向事务的高并发短读写数据库 |
| 10 | [OLAP](10-olap.md) | 列存、向量化、MPP、Cube | 面向分析的大规模聚合扫描引擎 |

## 第三批 · 企业级决策方案(11,总-分-分)

2026 年 PB 级流批一体湖仓的**决策级技术方案**。经 deep-research 对抗验证,标注置信度(🟢high / 🟡medium / 🔴refuted / ⚪open)。

| 编号 | 笔记 | 角色 |
|---|---|---|
| 11 | [**2026 流批一体技术方案(总纲)**](11-solution-2026.md) | **总** · 执行摘要 / 参考架构 / 逐层选型 / 三套场景 / 演进路线 |
| 11a | [架构范式](11a-architecture-paradigm.md) | 分 · Lambda / Kappa / Streaming Lakehouse |
| 11b | [存储与开放表格式](11b-storage-table-format.md) | 分 · Iceberg / Delta / Hudi / Paimon / Fluss / Unity Catalog |
| 11c | [计算与查询引擎](11c-compute-engine.md) | 分 · Flink / Spark / OLAP 引擎(多为开放问题 ⚪) |
| 11d | [分层与建模](11d-layered-modeling.md) | 分 · 实时分层 / 语义层 |
| 11e | [编排治理血缘](11e-governance-orchestration.md) | 分 · Dagster/Airflow / UC / OpenLineage / 数据契约 |

## 补充 · 第三大建模范式(12)

| 编号 | 笔记 | 主题 | 一句话 |
|---|---|---|---|
| 12 | [Data Vault 2.0](12-data-vault.md) | Hub / Link / Satellite、哈希键、Raw/Business Vault | 与 Inmon/Kimball 并列的第三大范式,面向审计/可扩展/多源集成 |

---

## 主题地图(如何串起来看)

```
建模方法论(怎么组织数据)
├─ 三大范式:01 Inmon(3NF) · 02 Kimball(维度) · 12 Data Vault(Hub/Link/Sat)
├─ 湖仓分层:03 Medallion(Bronze/Silver/Gold)
└─ 工具分层:04 dbt(staging/int/marts)

基础设施(数据存哪、算哪、用什么库)
├─ 存储:06 数据湖(开放表格式)
├─ 计算:07 离线(批) · 08 实时(流)
└─ 数据库:09 OLTP(事务/行存) · 10 OLAP(分析/列存)

编排与治理(谁来调度、怎么管)
└─ 05 Dagster(资产化编排 + 质量门)

企业级决策(2026 怎么选型落地)
└─ 11 总-分-分:流批一体 = 开放表格式地基 + Flink 统一处理
   + Paimon/Fluss 流式湖仓 + UC/OpenLineage/DataHub 治理 + dbt 语义层
```

## 方法论横向对比速查

| 维度 | Inmon(01) | Kimball(02) | Data Vault(12) | Medallion(03) | dbt(04) |
|---|---|---|---|---|---|
| 分层依据 | 主题+加工 | 业务过程 | 业务键+关系+属性 | 数据洁净度 | 转换职责 |
| 核心结构 | 3NF EDW | 事实/维度(星型) | Hub/Link/Satellite | Bronze/Silver/Gold | stg/int/marts |
| 方向 | 自顶向下 | 自底向上 | 集成层(非直面 BI) | 逐级提纯 | DAG 模块化 |
| 强项 | 一致性 | 查询易用 | 审计/可扩展/多源 | 湖仓通用 | 工程可维护 |

---

## 关于引用与置信度

- **01–10、12**:传统学习笔记,关键论断带脚注引用,文末列真实 URL。
- **11 系列**:经 `deep-research` workflow(扇出检索 → 抓取权威源 → 3 票对抗验证)产出,显式区分**已确认结论 / 被证伪断言(避坑清单) / 证据不足的开放问题**,并标注置信度。凡验证语料不足的角度(如计算引擎选型)如实标 ⚪,不做过度断言。

> 所有引用 URL 均来自真实检索结果。技术现状具时效性(尤其 11 系列的 2026 判断),落地前请按各篇的时效性声明二次验证。
