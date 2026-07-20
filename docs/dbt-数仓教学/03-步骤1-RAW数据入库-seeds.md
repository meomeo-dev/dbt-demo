# 03 · 第 1 步:RAW 数据入库(seeds)

> 对应代码:`seeds/jaffle-data/*.csv`(raw_customers / raw_orders / raw_items / raw_products / raw_supplies / raw_stores 共 6 张表)

## 这一步在整条链路的位置

```
[ CSV 文件 ] ──dbt seed──▶ [ MySQL analytics 库的表 ]  ← 你在这里
    seeds/jaffle-data/*.csv        raw_customers / raw_orders / raw_items
    (6 张 jaffle-shop 表)          raw_products / raw_supplies / raw_stores
```

真实的数仓里,原始(RAW)数据通常由外部管道(Fivetran、Airbyte、Kafka、批量导入等)灌进数仓,dbt **不负责**这一步的搬运。但在学习和 demo 场景下,dbt 提供了 **seed** 功能:把版本库里的 CSV 直接 `INSERT` 成数仓里的表,充当「原始数据源」。

## seed 是什么,不是什么

**seed 适合**:小体量、不常变、且适合进版本库的静态数据 —— 国家码表、状态枚举、测试用样例数据。本项目用它模拟「已经躺在数仓里的原始业务表」。

**seed 不适合**:大数据量、频繁更新、含敏感信息的生产数据。那些应该由专门的 EL(Extract-Load)工具灌入,dbt 只用 `source` 去引用(见 [第 4 章](04-步骤2-声明数据源-sources.md))。

## 本项目的 6 份 seed(jaffle-shop 数据集)

数据来自 dbt Labs 官方的 jaffle-shop(一个卖三明治 + 饮料的电商)示例,由 `jafgen` 生成后采样。6 张表构成一套完整的电商原始数据:

| 表 | 行数 | 列 | 说明 |
|----|------|----|------|
| `raw_customers` | 50 | id, name | 客户 |
| `raw_orders` | 300 | id, customer, ordered_at, store_id, subtotal, tax_paid, order_total | 订单(金额单位为**分**) |
| `raw_items` | 537 | id, order_id, sku | 订单行(一笔订单含多个下单商品) |
| `raw_products` | 10 | sku, name, type, price, description | 商品(菜单上的每个 SKU) |
| `raw_supplies` | 65 | id, name, cost, perishable, sku | 供应品(制作每个商品所需的耗材) |
| `raw_stores` | 6 | id, name, opened_at, tax_rate | 门店 |

引用完整性成立:`orders.customer ⊆ customers.id`、`items.order_id ⊆ orders.id`、`items.sku ⊆ products.sku`。

看两张核心表的表头样例:

```csv
# seeds/jaffle-data/raw_orders.csv —— 原始订单表(金额单位=分)
id,customer,ordered_at,store_id,subtotal,tax_paid,order_total
80bddaa0-...,a09b0afd-...,2018-09-01T08:58:00,9a428c06-...,5700,342,6042
...
```

```csv
# seeds/jaffle-data/raw_products.csv —— 原始商品表
sku,name,type,price,description
JAF-001,nutellaphone who dis?,jaffle,1100,nutella and banana jaffle
BEV-...,...,beverage,...,...
...
```

注意这里的字段名和值都是**原始的、未清洗的**风格:`customer`(而非统一的 `customer_id`)、`ordered_at`(datetime 字符串)、金额以「分」为整数存储、`perishable` 是字符串 `True`/`False`。清洗它们是下一层(staging)的活儿,seed 阶段**原样入库**。

## 为什么要在 dbt_project.yml 里声明列类型

CSV 没有类型信息,Trino/MySQL 对它的类型推断可能出错(比如把金额当成浮点)。本项目在 `dbt_project.yml` 里显式指定了几列的类型:

```yaml
seeds:
  dbt_demo:
    jaffle-data:
      raw_orders:
        +column_types:
          subtotal: integer       # 金额以"分"为单位存储(整数)
          tax_paid: integer
          order_total: integer
      raw_products:
        +column_types:
          price: integer
      raw_supplies:
        +column_types:
          cost: integer
          perishable: varchar     # 原始值 True/False,staging 层再转 boolean(演示类型转换)
```

## 执行

```bash
dbt seed --full-refresh
```

- 这会在 MySQL 的 `analytics` 库里建出 6 张 `raw_*` 表并灌入数据。
- **为什么用 `--full-refresh`**:不加时 dbt 只做增量追加;加上会先 `DROP` 再重建,保证表结构和数据与 CSV 完全一致。改了 CSV 后必须加这个参数,否则表不更新。

预期输出:`PASS=6`(6 张表都建成功)。

## 建好之后长什么样

`dbt seed` 之后,MySQL `analytics` 库里就有了 6 张真实的表。它们从 Trino 的视角看,就是 `mysql.analytics.raw_customers` 等(catalog.schema.table)。下一步我们**不直接引用这些表名**,而是先把它们「声明为 source」,这样 dbt 才能追踪血缘。

---

**上一章**:[02 · 连接配置 profiles.yml](02-连接配置-profiles.md) · **下一章**:[04 · 第 2 步 声明数据源](04-步骤2-声明数据源-sources.md)
