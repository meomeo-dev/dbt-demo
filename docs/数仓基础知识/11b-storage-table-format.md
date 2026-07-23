---
title: "11b · 存储层与开放表格式选型"
series: "数据平台技术方案 · 总-分-分"
doc_id: "11b"
role: "分支 / 详细论证"
parent: "11-solution-2026.md"
date: "2026-07"
---

# 11b · 存储层与开放表格式选型

> 本文是 [11 总纲](11-solution-2026.md) 的**存储与开放表格式**分支。结论标注置信度(🟢/🟡/🔴/⚪),源自 3 票对抗验证。
> 相关基础:[06-data-lake](06-data-lake.md) · [10-olap](10-olap.md)

## 1. 为何开放表格式是流批一体的地基 🟢

即便上了流式优先架构(diskless Kafka 等),SQL 分析、历史快照、治理工作流、跨引擎访问仍**要求开放表格式兜底**;推荐把 curated 流 sink 进 Apache Iceberg 类格式[1]。此点由 Kafka 厂商 AutoMQ 亲口确认(非自利),并获多方 2025–2026 独立来源印证。

## 2. 四大开放表格式对比

| 维度 | Iceberg | Delta Lake | Hudi | Paimon |
|---|---|---|---|---|
| 定位 | 批/分析事实标准 | Databricks 生态核心 | CDC / 流式 upsert | 流批一体原生 |
| 写入模型 | COW(+MOR 演进) | COW / Deletion Vectors | COW / MOR | LSM,MOR 强 |
| 流式读写 | Flink 连接器成熟 | streaming 支持 | CDC 定位 | **原生流批,Kappa-on-Lakehouse** |
| 2026 关键 | REST Catalog 事实标准化 | UniForm 互操作 | CDC/增量 | 分钟级更新[2] |

> ⚠️ **重要**:四格式**头对头排名**(写放大、CDC/upsert 吞吐、compaction 成本、REST Catalog 成熟度)在本轮验证中**证据不足**⚪。上表定性,不做量化排名——须实施时 POC 基准。

## 3. Paimon:湖仓上的 Kappa 载体 🟡

Paimon 直接操作对象存储文件,大规模 TB 更新达 **~1 分钟延迟**[2],阿里云官方 Flink 文档印证「分钟级延迟」。这是 2026 在湖仓上落地 Kappa 的主流路径。

## 4. Fluss + 表格式:Tiering + Union Reads 🟢(能力)

Fluss 的 Tiering Service 把秒级热数据**持续下沉到 Paimon/Iceberg/Lance**[3];Fluss + Paimon 暴露统一 catalog + 单表抽象,Flink Union Reads 合并实时与历史[3](阿里云官方文档独立印证)。⚠️ Fluss 处 Apache 孵化,成熟度是主要风险。

## 5. Catalog 层:Unity Catalog 的多接口互操作 🟢

Unity Catalog 通过**同一份数据上的三个开放接口**实现跨格式互操作[4]:

- **Delta Sharing** — 共享 Delta 表
- **Delta UniForm** — 让外部 Iceberg/Hudi 客户端读 Delta 表
- **Iceberg REST Catalog 接口** — 向 Iceberg 客户端暴露 catalog

UC 核心 API 及服务端/客户端实现**自 2024-06 起开源**(LF AI & Data),治理范围含**非结构化数据与 AI 模型**[4]。此结论有 VLDB 2025 白皮书 + GitHub + 官方文档多源印证,故 🟢。

## 6. PB 级工程难题(定性)

小文件治理、compaction 策略、Z-order/聚簇、分区演进、快照过期、并发写控制、元数据规模——这些是 PB 级共性挑战。⚠️ 各格式的具体处理优劣本轮无幸存量化断言,列为 ⚪。

## 7. 被证伪断言 🔴

| 断言 | 投票 | 说明 |
|---|---|---|
| Iceberg REST Catalog 能力有限、Polaris 仅支持 Iceberg 且无治理 API | 0-3 | Databricks 竞品贬低,被推翻 |
| 流式优先引擎不适配开放表格式(为批设计) | 0-3 | 与实践相悖 |

## 8. 给总纲的关键结论

- 开放表格式是地基,流式栈中必需。🟢
- 批/分析主格式选 **Iceberg**(事实标准 + REST Catalog);流主链路选 **Paimon**(Kappa-on-Lakehouse)。🟡
- Catalog 层 **Unity Catalog** 多接口互操作最强,已开源。🟢
- 四格式头对头排名、Delta/Hudi 取舍**证据不足**,须 POC。⚪

## 参考文献

1. AutoMQ:diskless Kafka 不替代湖仓 — https://www.automq.com/blog/lambda-vs-kappa-architecture-2026-diskless-kafka 🟡
2. Ververica + 阿里云 Flink 官方文档(Paimon 分钟级)— https://www.ververica.com/blog/from-kappa-architecture-to-streamhouse-making-lakehouse-real-time ;https://help.aliyun.com 🟡
3. Apache Fluss Tiering Service — https://fluss.incubator.apache.org/docs/next/streaming-lakehouse/tiering-service/ ;https://fluss.apache.org/blog/unified-streaming-lakehouse/ 🟢
4. Unity Catalog VLDB 2025 白皮书 — https://www.databricks.com/sites/default/files/2025-06/unity-catalog-open-universal-governance-lakehouse-beyond.pdf ;https://github.com/unitycatalog/unitycatalog ;https://docs.databricks.com/delta/uniform.html 🟢
