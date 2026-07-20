# 01 · dbt 是什么 & 项目结构

> 对应代码:`dbt_project.yml`

## dbt 在数仓里扮演什么角色

dbt(data build tool)是 **ELT 里的 "T"**(Transform,转换)。它不搬运数据、不做存储、不做计算 —— 这些是数仓(本项目里是 Trino + MySQL)的事。dbt 只做一件事:

> 让你用 **SQL + 一点 Jinja 模板**,以「软件工程的方式」管理数据转换 —— 版本控制、依赖管理、测试、文档、血缘,一应俱全。

你写的每个转换就是一个 `.sql` 文件(叫 **model**),里面是一句 `select`。dbt 负责把它包装成 `CREATE TABLE AS SELECT ...` 发给数仓执行,并根据模型之间的 `ref()` 引用**自动排出执行顺序**。

一句话对比:
- **没有 dbt**:一堆手写 SQL 脚本 + 手动记住谁先跑谁后跑 + 没测试 + 没文档。
- **有 dbt**:模型即代码,依赖自动推导,测试/文档/血缘内建。

## 项目目录结构

dbt 项目的目录是约定好的。本项目结构:

```
dbt-demo/
├── dbt_project.yml          # 项目配置(唯一必需的文件,项目的"根")
├── profiles.yml             # 连接配置(见第 02 章)
├── seeds/                   # CSV 种子数据(见第 03 章)
│   └── jaffle-data/         #   jaffle-shop 6 张 raw 表
│       ├── raw_customers.csv
│       ├── raw_orders.csv
│       ├── raw_items.csv
│       ├── raw_products.csv
│       ├── raw_supplies.csv
│       └── raw_stores.csv
├── models/                  # 所有转换模型(核心)
│   ├── staging/             #   清洗层(见第 05 章)
│   │   ├── sources.yml      #     source 声明 + 测试(见第 04 章)
│   │   ├── stg_schema.yml   #     staging 模型的文档与测试
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_products.sql
│   │   ├── stg_supplies.sql
│   │   └── stg_stores.sql
│   ├── intermediate/        #   中间层(见第 06 章)
│   │   ├── schema.yml
│   │   ├── int_supplies_aggregated_per_product.sql
│   │   ├── int_order_items_enriched.sql
│   │   ├── int_order_items_aggregated_to_order.sql
│   │   └── int_orders_aggregated_to_customer.sql
│   └── marts/               #   业务层(见第 07 章)
│       ├── schema.yml
│       ├── dim_customers.sql
│       ├── dim_products.sql
│       ├── dim_stores.sql
│       ├── fct_orders.sql
│       ├── fct_order_items.sql
│       └── fct_orders_incremental.sql
├── macros/                  # Jinja 宏(可复用的 SQL 片段,本项目暂空)
├── snapshots/               # 快照(SCD Type 2,本项目暂空)
├── tests/                   # singular 自定义测试(assert_order_total_positive.sql)
├── analyses/                # 一次性分析 SQL(不物化,本项目暂空)
└── target/                  # dbt 编译产物(自动生成,已 gitignore)
```

## dbt_project.yml 逐行讲

这是项目的「根配置」,dbt 靠它识别一个目录是不是 dbt 项目。

```yaml
name: 'dbt_demo'          # 项目名
version: '1.0.0'          # 项目版本
config-version: 2         # 配置文件格式版本(现代 dbt 都是 2)
profile: 'my_local_dwh'   # ← 用哪个 profile 连数仓(去 profiles.yml 找同名条目)

model-paths: ["models"]   # 各类文件的目录(通常保持默认)
seed-paths:  ["seeds"]
test-paths:  ["tests"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"     # 编译产物输出目录
clean-targets:            # dbt clean 时清空这些目录
  - "target"
  - "dbt_packages"

models:                   # ← 对模型的全局/分层配置
  dbt_demo:
    staging:
      +materialized: table    # staging 层统一 table 物化
    intermediate:
      +materialized: table    # intermediate 层统一 table 物化
    marts:
      +materialized: table    # marts 层统一 table 物化
```

重点看最后的 `models:` 块 —— 这是**按目录分层配置物化策略**的地方:

- `dbt_demo:` 对应项目名,下面的 `staging:` / `intermediate:` / `marts:` 对应 `models/` 下的子目录。
- `+materialized: table` 里的 `+` 前缀表示「这是一个配置项」(而非又一层目录)。它让该目录下所有模型默认用 table 物化。
- 普通 dbt 项目 staging 常用 `view`,**本项目因为 Trino MySQL connector 不支持 CREATE VIEW,强制用 table** —— 这是本项目最重要的约束之一(见 [第 05 章](05-步骤3-staging清洗层.md) 与 [附录](附录-命令速查与常见坑.md))。

## 五个你现在就该记住的概念

| 概念 | 一句话 | 本章之后详见 |
|------|--------|-------------|
| **model** | 一个 `.sql` 文件,内含一句 `select`,dbt 把它物化成表/视图 | 第 05、07 章 |
| **source** | 对「dbt 外部的原始表」的声明与引用 | 第 04 章 |
| **seed** | 把版本库里的 CSV 直接灌成数仓表 | 第 03 章 |
| **ref() / source()** | 模型间/对源的引用函数,dbt 靠它推导血缘和执行顺序 | 第 07 章 |
| **materialization** | 模型物化成什么(view/table/incremental/ephemeral) | 第 10 章 |

---

**上一章**:[00 · 数仓与 ELT 基础](00-数仓基础-ETL-vs-ELT.md) · **下一章**:[02 · 连接配置 profiles.yml](02-连接配置-profiles.md)
