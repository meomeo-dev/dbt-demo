# 04 · 第 2 步:声明数据源(sources)

> 对应代码:`models/staging/sources.yml`

## 为什么要「声明」source,而不是直接写表名

上一步 seed 建出了 `mysql.analytics.raw_customers`。你完全可以在 SQL 里直接 `select * from mysql.analytics.raw_customers`。但 dbt **强烈不推荐**这么做,原因有三:

1. **血缘可追踪**:声明为 source 后,dbt 的 DAG(血缘图)才知道「哪些模型依赖哪张原始表」。直接写死表名,血缘就断了。
2. **一处修改,处处生效**:库名/表名变了,只改 `sources.yml` 一处,所有 `{{ source(...) }}` 自动跟着变。
3. **可测试、可监控**:能对 source 直接挂测试(见下),还能做 **source freshness**(数据新鲜度)监控。

原则:**任何原始数据进入 dbt 的第一站,都应该先声明成 source,再被 staging 引用。**

## 本项目的 sources.yml 逐行讲解

本项目声明了 1 个 source(`raw`),下辖 6 张 jaffle-shop 原始表。下面截取关键片段:

```yaml
version: 2

sources:
  - name: raw                    # ← source 组名,后面 source('raw', ...) 的第一个参数
    description: "jaffle-shop 原始数据,由 dbt seed 从 CSV 载入 MySQL analytics schema"
    database: mysql              # Trino catalog 名(见第 02 章的术语错位)
    schema: analytics            # MySQL database 名
    tables:
      - name: raw_customers      # ← 表名,source('raw', 'raw_customers') 的第二个参数
        description: "原始客户表:每行一个客户"
        columns:
          - name: id
            tests: [unique, not_null]   # ← 直接对原始数据挂测试!

      - name: raw_orders
        description: "原始订单表:每行一笔订单(金额单位为分)"
        columns:
          - name: id
            tests: [unique, not_null]
          - name: customer
            description: "下单客户外键 → raw_customers.id"
            tests:
              - not_null
              - relationships:              # ← 外键完整性:每个 customer 都要能在客户表找到
                  to: source('raw', 'raw_customers')
                  field: id
          - name: store_id
            tests:
              - not_null
              - relationships:
                  to: source('raw', 'raw_stores')
                  field: id

      - name: raw_products
        description: "原始商品表:菜单上的每个 SKU"
        columns:
          - name: sku
            tests: [unique, not_null]
          - name: type
            tests:
              - not_null
              - accepted_values:            # ← 只能是这两种商品类型
                  arguments:
                    values: ['jaffle', 'beverage']
```

其余 `raw_items` / `raw_supplies` / `raw_stores` 同理声明。完整内容见源文件。

关键点:

- `name: raw` + `name: raw_customers` 组合起来,SQL 里就用 `{{ source('raw', 'raw_customers') }}` 引用。
- `database` / `schema` 定位到 Trino catalog 与 MySQL 库(和 `profiles.yml` 里一致)。
- **在 source 层就挂测试**是好习惯 —— 在数据「刚进门」时就校验主键唯一(`unique`)、非空(`not_null`)、外键完整(`relationships`)、枚举合法(`accepted_values`),能最早发现上游数据质量问题。这些测试就是后面 `dbt test` 时跑的一部分。
- **注意 `accepted_values` 的新语法**:dbt 较新版本把测试参数放进 `arguments:` 块下(`arguments: values: [...]`),本项目统一采用这种写法。

## source() 函数怎么被用

下一章的 `stg_customers.sql` 里第一句就是:

```sql
with source as (
    select * from {{ source('raw', 'raw_customers') }}
),
```

dbt 编译时会把 `{{ source('raw', 'raw_customers') }}` 替换成真实的 `mysql.analytics.raw_customers`。你写的是抽象引用,dbt 负责翻译成物理表名。

## 单独校验 source

```bash
dbt test --select source:raw          # 只跑 source 上挂的测试
```

---

**上一章**:[03 · 第 1 步 RAW 数据入库](03-步骤1-RAW数据入库-seeds.md) · **下一章**:[05 · 第 3 步 staging 清洗层](05-步骤3-staging清洗层.md)
