# dbt 数据仓库建模速成课程

> 从原始 RAW 数据到数仓落库的完整链路教学 —— 每一步都对应本项目里可运行的真实代码。
>
> **技术栈**:dbt Core 1.11 → dbt-trino 1.10.2 → Trino 482 → MySQL 9.7
> **配套代码**:本仓库 `models/`、`seeds/jaffle-data/`、`dbt_project.yml`、`profiles.yml`
> **数据集**:jaffle-shop(一个卖三明治+饮料的电商)6 张原始表 → 16 个模型 → 67 个测试

---

## 这门课教什么

这不是一份泛泛的 dbt 文档翻译。它把「一条数据从原始文件到数仓可用宽表」的**每一个环节**拆开讲,并且**每一步都指向本项目里真实存在、能跑起来的代码**,让你边读教程边读代码。

学完你会理解:

- 为什么现代数仓用 **ELT** 而不是 ETL,dbt 在其中扮演什么角色(Transform 层)
- 数仓怎么**分层**(source → staging → intermediate → marts),每层的职责边界
- dbt 的每个核心概念(`source()` / `ref()` / 物化策略 / 测试 / 文档 / 血缘)到底解决什么问题
- 一条数据如何从 `seeds/jaffle-data/raw_orders.csv` 流到 `marts/` 的星型模型(dim/fct 表)

## 阅读顺序(章节导航)

| # | 章节 | 讲什么 | 对应本项目代码 |
|---|------|--------|----------------|
| 00 | [数仓与 ELT 基础](00-数仓基础-ETL-vs-ELT.md) | ETL vs ELT、dbt 分层(可对照 Medallion)、Kimball 星型建模 | — |
| 01 | [dbt 是什么 & 项目结构](01-dbt-概念与项目结构.md) | dbt 定位、`dbt_project.yml`、目录约定 | `dbt_project.yml` |
| 02 | [连接配置 profiles.yml](02-连接配置-profiles.md) | profile/target、本项目 Trino 连接 | `profiles.yml` |
| 03 | [第 1 步:RAW 数据入库(seeds)](03-步骤1-RAW数据入库-seeds.md) | seed 的作用、CSV 落库 | `seeds/jaffle-data/*.csv` |
| 04 | [第 2 步:声明数据源(sources)](04-步骤2-声明数据源-sources.md) | `source()`、source freshness | `models/staging/sources.yml` |
| 05 | [第 3 步:staging 清洗层](05-步骤3-staging清洗层.md) | 一对一映射、重命名、类型规整 | `models/staging/stg_*.sql`(6 个) |
| 06 | [第 4 步:intermediate 中间层](06-步骤4-intermediate中间层.md) | 复用逻辑、拆分复杂转换(聚合/JOIN 两种模式) | `models/intermediate/int_*.sql`(4 个) |
| 07 | [第 5 步:marts 业务层](07-步骤5-marts业务聚合层.md) | 事实/维度、星型模型、`ref()` | `models/marts/dim_*.sql` / `fct_*.sql` |
| 08 | [第 6 步:测试与数据质量](08-步骤6-测试与数据质量.md) | 通用测试、singular test、扩展包 | 各 `schema.yml` + `tests/*.sql` |
| 09 | [第 7 步:文档与血缘 DAG](09-步骤7-文档与血缘.md) | `dbt docs`、description、lineage | 全项目 |
| 10 | [第 8 步:物化策略与增量上线](10-步骤8-物化策略与增量.md) | view/table/incremental/ephemeral、调度 | `dbt_project.yml`、`fct_orders_incremental.sql` |
| 11 | [完整数据流全景图](11-完整数据流全景图.md) | 从 RAW 到落库一张图串起所有步骤 | 全项目 |
| — | [命令速查 & 常见坑](附录-命令速查与常见坑.md) | 命令清单、dbt-trino 专属陷阱 | `CLAUDE.md` |
| — | [参考来源(权威一手资料)](附录-参考来源.md) | 全部 dbt 官方文档 URL(经 3-0 交叉验证) | — |

## 本项目数据流一览(先建立直觉)

```
seeds/jaffle-data/*.csv(6 张表)─┐
  raw_customers / raw_orders / raw_items          │  dbt seed(落库到 MySQL analytics schema)
  raw_products / raw_supplies / raw_stores ───────┘
        │
        ▼  声明为 source: raw.<6 张表>
  [ source 层 ] ── models/staging/sources.yml(挂 unique/not_null/relationships/accepted_values)
        │
        ▼  dbt run(清洗:重命名 + 类型转换 + 分转元)
  [ staging 层 ] ── stg_*.sql(6 个)                   (table 物化)
        │
        ▼  dbt run(聚合改粒度 + JOIN 打平)
  [ intermediate 层 ] ── int_supplies_aggregated_per_product / int_order_items_enriched
                        int_order_items_aggregated_to_order / int_orders_aggregated_to_customer(共 4 个)
        │
        ▼  dbt run(星型模型:维度 + 事实,纯 JOIN 组装)
  [ marts 层 ] ── dim_customers/products/stores + fct_orders/order_items   (table 物化)
        │
        ▼  dbt test(67 个:unique / not_null / relationships / accepted_values + 1 singular)
  [ 可信数据,供 BI/下游消费 ]
```

> 每一层如何对照 Medallion 术语、marts 层如何按 Kimball 星型建模,在 [第 00 章](00-数仓基础-ETL-vs-ELT.md) 和各步骤章节里逐一对应。

## 如何一边读一边跑

```bash
# 1. 起后端
cd deploy && docker compose up -d && cd ..

# 2. 按数据流顺序跑(顺序不可省,原因见附录)
dbt seed --full-refresh   # 第 1 步:RAW 入库(6 张表)
dbt run                   # 第 3~5 步:staging → intermediate → marts(16 个模型)
dbt test                  # 第 6 步:质量校验(67 个测试)
dbt docs generate && dbt docs serve   # 第 7 步:看血缘图
```

读到某一章时,打开右侧「对应本项目代码」列指向的文件对照着看,效果最好。
