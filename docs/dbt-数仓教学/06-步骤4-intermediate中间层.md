# 06 · 第 4 步:intermediate 中间层

> 对应代码:`models/intermediate/int_supplies_aggregated_per_product.sql`、`int_order_items_enriched.sql`、`int_order_items_aggregated_to_order.sql`、`int_orders_aggregated_to_customer.sql`(共 4 个)

## intermediate 层解决什么问题

intermediate(缩写 `int`)夹在 staging 和 marts 之间,职责是**承接不适合放在任何一张最终表里的中间转换**。典型触发信号:

1. **同一段逻辑被多个 marts 模型复用** —— 抽成一个 `int_` 模型,避免复制粘贴。
2. **单个 marts 模型太复杂** —— 一个 mart 里塞了五六个 JOIN + 多层聚合,读不懂也难测。拆成几个 `int_` 步骤,每步只做一件事。
3. **需要先 JOIN 再聚合的多阶段计算** —— staging 不许 JOIN,那这些跨表逻辑就落在 intermediate。

命名约定:`int_` 前缀,常按 `int_[entity]s_[verb]s` 组织(如本项目的 `int_supplies_aggregated_per_product`、`int_order_items_enriched`)。在其它数仓上物化常用 `ephemeral`(临时,不落表,只作为 CTE 内联进下游)或 view;**本项目因 Trino MySQL connector 不支持 view,intermediate 也统一用 table**。

## 一个关键教学点:改变粒度的聚合属于 intermediate,不属于 marts

marts 层的 `dim_` / `fct_` 表职责是**组装星型模型**(选外键、拼度量、做维度描述),它们的粒度应当由自身实体决定(每客户一行、每订单一行)。**凡是「改变粒度的聚合」——把多笔订单卷成每客户一行、把多条订单行卷成每订单一行——都应先在 intermediate 里做好,再让 marts 只做 JOIN 组装。**

为什么不把 `group by` 直接写进 `dim_customers` / `fct_orders`?

- **职责单一**:一张 mart 里既 `group by` 改粒度、又 JOIN 拼维度,逻辑纠缠、难读难测;拆到 int 后每个模型只做一件事。
- **可复用、可独立测试**:int 聚合结果有明确粒度和主键(客户级 / 订单级),可单独挂 `unique` + `not_null`,下游谁都能 `ref`。
- **避免 fact→fact 依赖**:订单级汇总若在 `fct_orders` 里直接引用 `fct_order_items`,就形成事实表依赖事实表的坏味道;下沉到 int 后,`fct_orders` 只依赖 int 层,依赖关系更干净。

## 本项目的 4 个 int_ 模型:聚合模式 vs JOIN 模式

本项目有两种典型的 intermediate 模式,共 4 个模型:

| 模型 | 模式 | 粒度变化 | 职责 |
|------|------|---------|------|
| `int_supplies_aggregated_per_product` | 聚合 | 多条供应品 → 每产品一行 | 汇总供应成本 |
| `int_order_items_enriched` | JOIN | 粒度不变(仍是订单行) | 打平订单行 + 补售价/成本/毛利 |
| `int_order_items_aggregated_to_order` | 聚合 | 多条订单行 → 每订单一行 | 汇总商品数/成本/毛利 |
| `int_orders_aggregated_to_customer` | 聚合 | 多笔订单 → 每客户一行 | 汇总累计订单/消费/首末单日期 |

- **聚合模式(3 个)**:带 `group by`,**改变粒度**——把细粒度的行卷成粗粒度的一行。
- **JOIN 模式(1 个)**:`int_order_items_enriched`,**不改粒度**,只是把多张表横向拼宽(补列)。

### 模式一:聚合 · 供应成本 → 产品粒度(int_supplies_aggregated_per_product)

把 `stg_supplies`(每行一个耗材)按产品聚合成「每产品一行的总成本」——**改变了粒度**:

```sql
-- int_supplies_aggregated_per_product.sql:模式 = "aggregate"
with supplies as (
    select * from {{ ref('stg_supplies') }}
),

aggregated as (
    select
        product_sku,
        count(*)            as supply_count,       -- 该产品用到的供应品种类数
        sum(supply_cost)    as total_supply_cost   -- 制作该产品的总耗材成本
    from supplies
    group by product_sku
)

select * from aggregated
```

这段成本聚合被 `dim_products` 和 `int_order_items_enriched` 两处复用 —— 正是「多个下游复用」这一信号,值得抽成独立的 intermediate 模型。

### 模式二:JOIN 打平(int_order_items_enriched)

staging 不许 JOIN,跨表拼接的活儿落在这里。把订单行打平,补齐产品售价与供应成本,并算出毛利:

```sql
-- int_order_items_enriched.sql:模式 = "join"
with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

supply_costs as (
    select * from {{ ref('int_supplies_aggregated_per_product') }}   -- int 可引用 int
),

joined as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.product_sku,
        p.product_name,
        p.product_type,
        p.is_food_item,
        p.product_price,                                        -- 该行售价
        coalesce(sc.total_supply_cost, 0)   as supply_cost,     -- 该行成本
        p.product_price
            - coalesce(sc.total_supply_cost, 0) as gross_margin -- 毛利 = 售价 - 成本
    from order_items oi
    left join products     p  on oi.product_sku = p.product_sku   -- 显式 ON,不用 USING
    left join supply_costs sc on oi.product_sku = sc.product_sku
)

select * from joined
```

两个要点:

- **`int` 可以 `ref` 另一个 `int`**:这里 `int_order_items_enriched` 引用了 `int_supplies_aggregated_per_product`,dbt 靠 `ref()` 自动排出「先建成本聚合,再建打平表」的顺序。
- **⚠️ 用显式 `on`,不用 `using`**:Trino 上的硬约束,后面第 07 章还会强调。

### 模式三:聚合 · 订单行 → 订单粒度(int_order_items_aggregated_to_order)【新】

把 `int_order_items_enriched`(每行一个下单商品)按订单聚合成「每订单一行的商品数/总成本/总毛利」——**改变了粒度**(订单行 → 订单)。这些汇总度量以前内联在 `fct_orders` 里,现已下沉到 int 层:

```sql
-- int_order_items_aggregated_to_order.sql:模式 = "aggregate"
--   从 int_order_items_enriched 聚合,而非 fct_order_items,以避免 fact→fact 依赖
with order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

aggregated as (
    select
        order_id,
        count(*)            as item_count,          -- 该订单包含多少个商品
        sum(supply_cost)    as order_supply_cost,   -- 该订单总成本
        sum(gross_margin)   as order_gross_margin   -- 该订单总毛利
    from order_items
    group by order_id
)

select * from aggregated
```

**为什么从 `int_order_items_enriched` 聚合、而不是从 `fct_order_items`?** 若从事实表聚合再喂给另一张事实表 `fct_orders`,就形成 fact→fact 依赖(坏味道)。改从 int 层聚合,`fct_orders` 只依赖本 int 模型,依赖图更干净。产出主键 `order_id`,挂 `unique` + `not_null`。

### 模式四:聚合 · 订单 → 客户粒度(int_orders_aggregated_to_customer)【新】

把 `stg_orders`(每行一笔订单)按客户聚合成「每客户一行的累计指标」——**改变了粒度**(订单 → 客户)。这些累计指标以前内联在 `dim_customers` 里,现已下沉到 int 层:

```sql
-- int_orders_aggregated_to_customer.sql:模式 = "aggregate"
with orders as (
    select * from {{ ref('stg_orders') }}
),

aggregated as (
    select
        customer_id,
        count(*)            as lifetime_orders,        -- 累计订单数
        sum(order_total)    as lifetime_spend,         -- 累计消费
        min(ordered_date)   as first_order_date,       -- 首单日期
        max(ordered_date)   as most_recent_order_date  -- 末单日期
    from orders
    group by customer_id
)

select * from aggregated
```

产出主键 `customer_id`,供 `dim_customers` 组装为「customer-360」富维度;同样挂 `unique` + `not_null`。

> 小结:3 个聚合模型(`*_per_product` / `*_to_order` / `*_to_customer`)都改变粒度,是「聚合模式」;`int_order_items_enriched` 不改粒度只补列,是「JOIN 模式」。**记住:改粒度的聚合放 int,marts 只做 JOIN 组装。**

## intermediate 在 dbt_project.yml 里的配置

```yaml
models:
  dbt_demo:
    staging:
      +materialized: table
    intermediate:
      +materialized: table    # 本项目 Trino 不支持 view/ephemeral 落 view,仍用 table
    marts:
      +materialized: table
```

## 执行

```bash
dbt run --select intermediate     # 只跑 intermediate 层(4 个模型)
dbt run --select int_order_items_enriched
```

---

**上一章**:[05 · 第 3 步 staging 清洗层](05-步骤3-staging清洗层.md) · **下一章**:[07 · 第 5 步 marts 业务聚合层](07-步骤5-marts业务聚合层.md)
