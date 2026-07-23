---
title: "11d · 流批一体下的分层与建模"
series: "数据平台技术方案 · 总-分-分"
doc_id: "11d"
role: "分支 / 详细论证"
parent: "11-solution-2026.md"
date: "2026-07"
---

# 11d · 流批一体下的分层与建模

> 本文是 [11 总纲](11-solution-2026.md) 的**分层与建模**分支。结论标注置信度(🟢/🟡/🔴/⚪),源自 3 票对抗验证。
> 相关基础:[01-inmon](01-inmon.md) · [02-kimball](02-kimball.md) · [03-medallion](03-medallion.md) · [04-dbt](04-dbt.md)

## ⚠️ 本角度证据充分性声明(必读)

本轮 deep-research 在**物理分层建模**上的验证语料**偏薄**:ODS/DWD/DWS/ADS 实时化的具体落地、与 Medallion 的物理映射、Kimball vs Data Vault 在流批下的取舍——**均无幸存的经核查断言**[openQuestion]。

**唯一获强验证的是「语义层」**(dbt Semantic Layer / MetricFlow)🟢。因此本文对语义层展开论证,对物理分层设计**只给方向、标 ⚪**,并复用前 10 篇的原理引用,不编造实时分层的"最佳实践"数字。

## 1. 语义层:dbt Semantic Layer / MetricFlow 🟢

dbt Semantic Layer 由开源(**Apache 2.0**)的 **MetricFlow** 引擎驱动,在既有 dbt 模型之上从 YAML 语义模型/指标定义**集中定义指标**,解析数据位置,并在**查询时生成仓库 SQL**——包括**按实体键类型自动构建 JOIN 以避免 fan-out / chasm join**——而**非物化预计算指标表**[1]。

这解决了流批一体下的核心痛点之一:**指标口径一致**。无论底层是实时还是离线链路,指标定义单点收敛、查询时统一生成 SQL,避免流批双套口径漂移。有 dbt 官方文档 + GitHub LICENSE 多源印证,🟢。

> 可选的 "Exports" 能物化结果,但**默认/核心行为是查询时 SQL 生成**[1]。

## 2. 分层挑战(方向性,⚪)

流批一体下传统分层面临的挑战(原理见 [03-medallion](03-medallion.md) / [01-inmon](01-inmon.md)):

- 传统离线 **ODS/DWD/DWS/ADS** 如何实时化 ⚪
- **Medallion(Bronze/Silver/Gold)** 在流式场景的物理落地形态 ⚪
- 流批**口径一致**、状态与回溯、**实时 SCD**、数据复用 ⚪
- 建模范式:**Kimball 维度建模 vs Data Vault 2.0 vs One Big Table(宽表)** 在 PB 级如何选 ⚪

> ⚠️ 以上均无本轮幸存验证断言支撑。**不做"最佳实践"断言**,建议结合 [02-kimball](02-kimball.md)(维度建模原理)、[03-medallion](03-medallion.md)(奖章分层原理)与厂商 2025–2026 实践文档,在自身场景 POC。

## 3. 被证伪断言 🔴

| 断言 | 投票 | 说明 |
|---|---|---|
| Medallion 中 Paimon 每层累积 checkpoint 延迟致 Gold 层 3min | 1-2 | 项目博客自述,未获印证(详见 [11a](11a-architecture-paradigm.md)) |

## 4. 分层与存储/引擎映射(定性)

结合已验证的存储/引擎结论(见 [11b](11b-storage-table-format.md)/[11c](11c-compute-engine.md)):

| 分层(概念) | 存储落点 | 处理引擎 |
|---|---|---|
| 原始/接入(Bronze/ODS) | Fluss 热层(可选)→ Paimon/Iceberg | Flink |
| 规范化/明细(Silver/DWD) | Paimon(流)/ Iceberg(批) | Flink / Spark |
| 服务/聚合(Gold/DWS/ADS) | Iceberg + OLAP 服务层 | Flink + OLAP ⚪ + dbt 语义层 🟢 |

> 存储/引擎落点有 [11b](11b-storage-table-format.md)/[11c](11c-compute-engine.md) 的验证支撑;分层名称与物理设计的精确映射属 ⚪。

## 5. 给总纲的关键结论

- **语义层选 dbt Semantic Layer / MetricFlow**:集中指标、查询时生成带自动 JOIN 的 SQL、避免 fan-out/chasm join、解决流批口径一致。🟢
- 物理分层(ODS/DWD/DWS/ADS 实时化、Medallion 映射、Kimball vs Data Vault)**本轮证据不足,列为开放问题**。⚪
- 分层到存储/引擎的映射可参考 [11b](11b-storage-table-format.md)/[11c](11c-compute-engine.md) 的已验证落点。

## 参考文献

1. dbt Semantic Layer 工作原理 — https://www.getdbt.com/blog/how-the-dbt-semantic-layer-works ;MetricFlow 文档 https://docs.getdbt.com/docs/build/about-metricflow ;SL 架构 https://docs.getdbt.com/docs/use-dbt-semantic-layer/sl-architecture ;MetricFlow GitHub https://github.com/dbt-labs/metricflow 🟢
2. 维度建模 / 奖章分层原理见本系列 [02-kimball](02-kimball.md) · [03-medallion](03-medallion.md)(独立文献)
