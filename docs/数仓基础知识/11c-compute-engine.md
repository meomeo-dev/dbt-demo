---
title: "11c · 计算与查询引擎选型"
series: "数据平台技术方案 · 总-分-分"
doc_id: "11c"
role: "分支 / 详细论证"
parent: "11-solution-2026.md"
date: "2026-07"
---

# 11c · 计算与查询引擎选型

> 本文是 [11 总纲](11-solution-2026.md) 的**计算与查询引擎**分支。结论标注置信度(🟢/🟡/🔴/⚪),源自 3 票对抗验证。
> 相关基础:[07-batch-computing](07-batch-computing.md) · [08-stream-computing](08-stream-computing.md) · [10-olap](10-olap.md)

## ⚠️ 本角度证据充分性声明(必读)

本轮 deep-research 的验证语料在**计算/查询引擎选型**上**偏薄到缺失**:没有幸存的经核查断言为 Flink vs Spark、或 StarRocks/Doris/ClickHouse/Trino/DuckDB vs 云数仓提供**决策标准或基准**[openQuestion]。

因此本文**不编造量化对比或基准数字**。凡下方定性判断,均标 ⚪(待实施时 POC 基准),并明确与前 10 篇原理笔记(已有独立证据)的引用区分开来。这是**忠实呈现研究边界**,而非知识空白。

## 1. 引擎分类(定性框架)

```
处理引擎(Processing)        服务/查询引擎(Serving/Query)
├─ Flink(真流+批)          ├─ 实时 OLAP:StarRocks / Doris / ClickHouse / Druid
├─ Spark(Structured        ├─ 联邦查询:Trino / Presto
│   Streaming + 批)         ├─ 嵌入式:DuckDB
                            └─ 云数仓:Snowflake / BigQuery / Databricks SQL
```

## 2. 处理层:Flink vs Spark 🟡

结合 [11a](11a-architecture-paradigm.md) 已验证的证据:

- **Flink** — 真流 + 批,原生 **Union Reads** 合并实时/历史(与 Paimon/Fluss 协同,已验证 🟢),是 2026 流批一体处理层首选。🟡
- **Spark** — 大规模批、回溯 backfill、生态成熟,作处理层备选。🟡

> 这两条判断的**协同能力**部分(Flink + Paimon/Fluss)有独立印证;但「Flink 优于 Spark」这类**排序性结论无幸存基准支撑**,故整体标 🟡,PB 级取舍须 POC。

## 3. 服务层:实时 OLAP 引擎 ⚪

StarRocks / Doris / ClickHouse / Druid 的架构、并发、成本头对头对比——**本轮无幸存验证断言**。请勿据本文选型;应参考:

- 各引擎官方文档与最新(2025–2026)独立基准
- 真实工作负载 POC(并发、扫描量、成本口径需一致)

原理层面(列存、向量化、MPP)见 [10-olap](10-olap.md)(该篇有独立文献支撑)。

## 4. 湖上查询与嵌入式

- **Trino / Presto、StarRocks/Doris 直查 Iceberg** — 湖上查询是趋势,但选型证据不足 ⚪。
- **DuckDB(嵌入式)** — 适合单机/中小规模分析。**本项目 tushare-dashboard 的 `backend/dbt` 正是用 dbt-duckdb**,DuckDB 作嵌入式 OLAP 变换引擎,是「小规模不必上重型 MPP」的实证案例(见 [10-olap](10-olap.md) 的 DuckDB 实战)。⚪(定位判断,非基准)

## 5. 引擎与开放表格式协同 🟢(部分)

已验证的协同事实:Flink 通过 Union Reads 高效读写 Paimon + Fluss(阿里云官方文档印证,见 [11a](11a-architecture-paradigm.md)/[11b](11b-storage-table-format.md))🟢。其余引擎对 Iceberg/Paimon 的读写效率排名 ⚪。

## 6. 给总纲的关键结论

- 处理层**首选 Flink**(与 Paimon/Fluss 协同已验证),Spark 作大规模批/回溯备选。🟡
- 服务层 OLAP 引擎(StarRocks/Doris/ClickHouse/Trino/DuckDB)**本轮证据不足,列为开放问题,须 POC 基准**。⚪
- 小规模场景 DuckDB 嵌入式够用(本项目实证)。⚪
- 切勿据未验证的营销基准做 PB 级引擎决策。

## 参考文献

> 本角度**缺乏可引用的经核查基准来源**(见开头声明)。处理层与表格式协同的引用见:
1. Apache Fluss / Ververica / 阿里云 Flink 文档(Flink + Paimon/Fluss 协同)— https://fluss.apache.org/blog/unified-streaming-lakehouse/ ;https://help.aliyun.com 🟢
2. OLAP 原理与引擎概览(独立文献)见本系列 [10-olap](10-olap.md) 参考文献
