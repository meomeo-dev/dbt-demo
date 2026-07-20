# 05 · 第 3 步:staging 清洗层

> 对应代码:`models/staging/stg_*.sql`(6 个模型:stg_customers / stg_orders / stg_order_items / stg_products / stg_supplies / stg_stores)

## staging 层的唯一职责:把原始数据「洗干净、摆整齐」

staging 是原始数据进入 dbt 后的**第一个转换层**。它的规矩很严,记住这几条边界:

**staging 该做的**:
- **一对一映射**:一个 source 表 → 一个 stg 模型,不多不少。
- **重命名**:把原始的、随意的字段名统一成规范命名(`name` → `customer_name`)。
- **类型转换**:字符串转日期/数值(`cast(... as date)`)。
- **轻度清洗**:去首尾空格、统一大小写(`trim(name)`、`lower(...)`)。

**staging 绝对不该做的**:
- ❌ **JOIN**(连表)—— 那是 intermediate/marts 的活儿。
- ❌ **聚合**(`group by`、`sum`)—— 同上。
- ❌ 业务逻辑判断。

> 一句话:staging 只负责「让每张表自己变干净」,不负责「表和表之间的关系」。这样上层模型才有一个统一、可信的地基。

## 命名约定:`stg_` 前缀

dbt 官方 style guide 约定 staging 模型以 `stg_` 开头,通常还会带上来源系统前缀(如 `stg_stripe__payments`)。本项目源单一,直接用 `stg_customers` / `stg_orders` / `stg_products` 等,一张源表对应一个 stg 模型,共 6 个。

## stg_customers.sql 逐段讲

```sql
-- staging: 清洗 raw_customers(一对一映射,只重命名,不 JOIN 不聚合)
with source as (
    select * from {{ source('raw', 'raw_customers') }}   -- ① 引用上一章声明的 source
),

renamed as (
    select
        id           as customer_id,       -- ② 重命名:id → 语义化的 customer_id
        trim(name)   as customer_name       -- ③ 轻度清洗:去首尾空格
    from source
)

select * from renamed
```

动作全都在「单表」范围内,没有一个 JOIN,没有一个聚合 —— 这就是标准的 staging 写法。用 CTE(`with ... as`)分段命名(`source` → `renamed`)是 dbt 社区的通行风格,可读性好、便于调试。

## stg_orders.sql:重命名 + 类型转换 + 分转元

```sql
with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

renamed as (
    select
        id                        as order_id,
        customer                  as customer_id,   -- 重命名为统一的外键名
        store_id,
        cast(ordered_at as date)  as ordered_date,  -- datetime → date
        subtotal    / 100.0       as subtotal,      -- 分 → 元
        tax_paid    / 100.0       as tax_paid,
        order_total / 100.0       as order_total
    from source
)

select * from renamed
```

金额从「分」除以 100.0 转成「元」,属于官方允许的 staging 轻度计算。

## 其余 4 个 staging 模型

- **stg_order_items**:清洗 `raw_items` → `order_item_id`(由 id 重命名)、`order_id`、`product_sku`(由 sku 重命名)。
- **stg_products**:`raw_products` → `product_sku` / `product_name` / `product_type` / `product_description`,`price / 100.0 as product_price`,并派生 `is_food_item = (type = 'jaffle')`(分类归桶,属官方允许的 staging 转换)。
- **stg_supplies**:`raw_supplies` → `supply_id` / `product_sku` / `supply_name`,`cost / 100.0 as supply_cost`,并把字符串 `'true'/'false'` 转成真正的 boolean `is_perishable`(演示类型规整)。
- **stg_stores**:`raw_stores` → `store_id` / `store_name`,`cast(opened_at as date) as opened_date`、`tax_rate`。

这些都是「单表清洗」,没有 JOIN 也没有聚合。跨表拼接和改变粒度的活儿留给下一层(intermediate)。

## ⚠️ 本项目特有约束:staging 必须 `table` 物化

普通 dbt 项目里,staging 层默认用 **view**(视图)物化 —— 轻量、不占存储、总是最新。但本项目跑在 **Trino MySQL connector** 上,**它不支持 `CREATE VIEW`**。所以 `dbt_project.yml` 强制把 staging 设成 table:

```yaml
# dbt_project.yml
models:
  dbt_demo:
    staging:
      +materialized: table    # ← 不能用 view,Trino MySQL connector 限制
    intermediate:
      +materialized: table
    marts:
      +materialized: table
```

物化策略的完整讲解见 [第 10 章](10-步骤8-物化策略与增量.md)。这里只需记住:本项目所有层都用 table。

## 执行与验证

```bash
dbt run --select staging          # 只跑 staging 层(6 个模型)
dbt run --select stg_customers    # 只跑单个模型
```

跑完后,MySQL `analytics` 库里会多出 6 张 `stg_*` 表,字段已是清洗后的规范命名。

---

**上一章**:[04 · 第 2 步 声明数据源](04-步骤2-声明数据源-sources.md) · **下一章**:[06 · 第 4 步 intermediate 中间层](06-步骤4-intermediate中间层.md)
