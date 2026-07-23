---
title: "11e · 编排、治理、元数据、数据质量与血缘"
series: "数据平台技术方案 · 总-分-分"
doc_id: "11e"
role: "分支 / 详细论证"
parent: "11-solution-2026.md"
date: "2026-07"
---

# 11e · 编排、治理、元数据、数据质量与血缘

> 本文是 [11 总纲](11-solution-2026.md) 的**编排治理血缘**分支。结论标注置信度(🟢/🟡/🔴/⚪),源自 3 票对抗验证。**本角度是本轮验证证据最扎实的分支之一。**
> 相关基础:[05-dagster](05-dagster.md) · [04-dbt](04-dbt.md)

## 1. 治理/目录:Unity Catalog 多接口互操作 🟢

Unity Catalog 通过**同一份数据上的三个开放接口**实现跨格式互操作[1]:

| 接口 | 作用 |
|---|---|
| **Delta Sharing** | 共享 Delta 表 |
| **Delta UniForm** | 让外部 Iceberg/Hudi 客户端读 Delta 表 |
| **Iceberg REST Catalog 接口** | 向 Iceberg 客户端暴露 catalog |

- UC 核心 API 与服务端/客户端实现**自 2024-06 起开源**(LF AI & Data)[1]。
- 治理范围**超越表**,含**非结构化数据与 AI 模型**[1]。

有 VLDB 2025 白皮书 + GitHub + 官方文档多源印证,🟢。

> 🔴 避坑:「Iceberg REST Catalog 能力有限、Polaris 仅支持 Iceberg 且无治理 API」这一断言在验证中被 **0-3 推翻**,系竞品贬低框架,勿采信。

## 2. 血缘:OpenLineage 标准 🟢

OpenLineage 是**开放、可扩展的血缘采集与分析规范框架**,让各系统就血缘元数据互操作,是管线血缘的事实集成层[2]。

- **Apache Airflow OpenLineage provider** 通过注册 **Airflow Listener(AirflowPlugin)** 自动采集血缘,**无需改动用户 DAG 文件**,仅需配置事件发送目标[2]。
- 原生 emitter 覆盖 Airflow/Spark/Flink/dbt,可被 DataHub/Marquez 摄取。

> ⚠️ 细粒度的按算子血缘可能仍需算子级 extractor。整体结论 🟢(官方文档印证)。

## 3. 元数据平台:DataHub 的 Dagster 集成 🟢

DataHub 的 Dagster 集成以 **Dagster Sensor** 运行(在 `acryl_datahub_dagster_plugin` 中,从 Dagster UI 的 Sensors 页开关)[3]:

- 每次管线运行后**发送元数据**:pipeline/task 元数据、运行状态、表血缘(验证于 **Dagster 1.7.0+**)[3]
- **Asset materialization 追踪**:在 `AssetMaterialization` 事件上把 Dagster asset key 映射为 DataHub Dataset,由 `capture_asset_materialization`(默认 True)控制[3]

有 DataHub 官方文档 + PyPI 多源印证,🟢。

> 本项目 tushare-dashboard 用 **Dagster + dbt + quality 三层资产 + catalog**(见 [05-dagster](05-dagster.md)),是小规模「asset-based 编排 + 质量门」的参照案例。

## 4. 数据契约:向 ODCS v3.1.0 收敛 🟢

数据契约定义 provider 与 consumers 间交换数据的**结构、格式、语义、质量与使用条款**,明确面向 **data mesh 与计算式治理**;并经 **OpenLineage Column Level Lineage Dataset Facet** 内嵌**字段级血缘**(输入字段 namespace/name/field;变换类型 DIRECT/INDIRECT,含 JOIN、GROUP_BY、FILTER 等子类)[4]。

**关键 2026 动向**:旧的 **Data Contract Specification(v1.2.1)已弃用**,收敛到 **Open Data Contract Standard(ODCS)v3.1.0**;工具支持仅到 **2026 年底**,官方建议迁移——标志行业向**单一标准**收敛[4]。🟢

## 5. 编排层:Dagster vs Airflow 🟡

| | Dagster | Airflow |
|---|---|---|
| 范式 | asset-based(声明数据资产)| task-based(声明任务 DAG)|
| 血缘 | 原生 asset 血缘 + DataHub Sensor 🟢 | OpenLineage Listener 自动采集 🟢 |
| 流批统一编排 | asset + sensor 触发 | provider 生态广 |

> 两者的编排能力对比属工程判断(🟡);但各自的**血缘集成事实**(§2/§3)是 🟢。原理见 [05-dagster](05-dagster.md)。

## 6. 数据质量 ⚪/🟡

Great Expectations / Soda / dbt tests / 流式质量门——本轮**无针对性幸存验证断言**,列为方向性 ⚪。本项目以 `backend/app/quality/*` + Dagster asset_check 做质量门(见 [05-dagster](05-dagster.md)),是小规模质量门实证。

## 7. 给总纲的关键结论

- **治理/目录首选 Unity Catalog**:三接口(Delta Sharing / UniForm / Iceberg REST Catalog)互操作,已开源,治理含非结构化 + AI 资产。🟢
- **血缘用 OpenLineage**:开放标准,Airflow 零改 DAG 自动采集。🟢
- **元数据平台 DataHub**:一方 Dagster Sensor(1.7.0+),运行后发元数据、映射 asset→Dataset。🟢
- **数据契约迁移到 ODCS v3.1.0**:旧规范已弃用,工具支持仅到 2026 底。🟢
- 编排 Dagster/Airflow 按团队与流批需求选;数据质量工具选型证据不足 ⚪。

## 参考文献

1. Unity Catalog VLDB 2025 白皮书 — https://www.databricks.com/sites/default/files/2025-06/unity-catalog-open-universal-governance-lakehouse-beyond.pdf ;https://github.com/unitycatalog/unitycatalog ;https://docs.databricks.com/delta/uniform.html 🟢
2. Airflow OpenLineage provider — https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/structure.html ;https://openlineage.io 🟢
3. DataHub Dagster 集成 — https://datahubproject.io/docs/lineage/dagster/ ;https://docs.datahub.com/docs/lineage/dagster/ ;https://pypi.org/project/acryl-datahub-dagster-plugin/ 🟢
4. Data Contract Specification(已弃用)— https://datacontract-specification.com/ ;ODCS 官方 https://bitol-io.github.io ;OpenLineage 列级血缘 https://openlineage.io 🟢
