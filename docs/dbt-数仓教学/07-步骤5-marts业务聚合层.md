# 07 · 第 5 步:marts 业务聚合层

> 对应代码:`models/marts/dim_customers.sql`、`dim_products.sql`、`dim_stores.sql`、`fct_orders.sql`、`fct_order_items.sql`

## marts 层:面向业务的「最终成品」

marts(数据集市)是数仓给下游(BI 报表、分析师、应用)直接消费的一层。特点:

- **面向业务问题**,而非面向源系统。一张 mart 通常回答一个业务问题:「每个客户下了多少单、花了多少钱?」「每个订单的毛利是多少?」
- **可以 JOIN、可以聚合** —— staging 不许做的,这里放开做。
- 通常组织成 **事实表(fact)** 和 **维度表(dim)**(Kimball 星型模型,见 [第 00 章](00-数仓基础-ETL-vs-ELT.md)),命名用 `fct_` / `dim_` 前缀。

本项目的 marts 层严格按星型模型拆分成 5 张表:

| 表 | 类型 | 粒度 | 主要内容 |
|----|------|------|---------|
| `dim_customers` | 维度 | 每客户一行 | 客户属性 + 累计订单指标(来自 int) |
| `dim_products` | 维度 | 每 SKU 一行 | 售价、成本、毛利 |
| `dim_stores` | 维度 | 每门店一行 | 门店属性 |
| `fct_orders` | 事实 | 每订单一行 | 金额度量 + 订单行汇总的商品数/成本/毛利(来自 int) |
| `fct_order_items` | 事实 | 每下单商品一行(最细) | 售价/成本/毛利,外键指向 dim/fct |

> **marts 层的职责是「组装星型模型」**:选外键、拼度量、做维度描述。**凡是改变粒度的聚合(订单→客户、订单行→订单)都已下沉到 intermediate 层**(见 [第 06 章](06-步骤4-intermediate中间层.md)),marts 里的 `dim_` / `fct_` 只做 `ref` int 结果 + `left join` 组装,**本身不再写 `group by`**。

## dim_customers.sql:纯 JOIN 组装(累计指标来自 int)

维度表描述「谁」。客户的累计订单指标(下单数、消费、首末单日期)由 `int_orders_aggregated_to_customer` 聚合好,这里只把它 JOIN 到客户属性上组装成富维度——**没有 `group by`**:

```sql
with customers as (
    select * from {{ ref('stg_customers') }}    -- ① ref() 引用 staging 模型
),

customer_orders as (
    select * from {{ ref('int_orders_aggregated_to_customer') }}  -- ② 累计指标已在 int 聚合好
),

final as (
    select
        c.customer_id,
        c.customer_name,
        coalesce(co.lifetime_orders, 0)  as lifetime_orders,   -- ③ coalesce 处理无订单客户
        coalesce(co.lifetime_spend, 0)   as lifetime_spend,
        co.first_order_date,
        co.most_recent_order_date,
        case when co.lifetime_orders is null then false
             else true end              as is_active_customer   -- 是否活跃客户
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id   -- ④ 显式 ON,不用 USING!
)

select * from final
```

**关键:`ref()` vs `source()`**
- `{{ source(...) }}` 引用「dbt 外部的原始表」(seed / EL 灌入的表)。
- `{{ ref(...) }}` 引用「另一个 dbt 模型」。
- dbt 靠这两个函数构建整张血缘 DAG,并**自动推导构建顺序** —— 你永远不用手动排顺序。

三个要点:
- **不做聚合**:客户级累计指标已由 `int_orders_aggregated_to_customer` 算好;这张 dim 只做 JOIN 组装。「改变粒度的聚合放 int、marts 只组装」是本项目的分层原则。
- **`coalesce(..., 0)`**:用 `left join` 保证「一单都没下的客户也出现在结果里」,他们的聚合值为 NULL,`coalesce` 把 NULL 兜底成 0。
- **⚠️ 用显式 `on`,不用 `using`**:这是本项目 Trino 上的硬约束。Trino 里 `LEFT JOIN ... USING (customer_id)` 会把连接列合并成一个无表别名的列,之后 `c.customer_id` / `co.customer_id` 都无法解析、直接报错。**一律写 `on c.customer_id = co.customer_id`。**

## 两张事实表

### fct_order_items(最细粒度)

订单行事实表,每行一个下单商品。它把 intermediate 的打平结果与订单信息(客户、门店、日期)拼在一起,度量是售价/成本/毛利:

```sql
with order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select
        oi.order_item_id,
        oi.order_id,
        o.customer_id,
        o.store_id,
        o.ordered_date,
        oi.product_sku,
        oi.product_price,          -- 度量:售价
        oi.supply_cost,            -- 度量:成本
        oi.gross_margin            -- 度量:毛利
    from order_items oi
    left join orders o on oi.order_id = o.order_id   -- 显式 ON
)

select * from final
```

> **纯星型**:事实表只存外键 + 度量,描述属性(如产品名 `product_name`、类型 `product_type`)查询时 JOIN `dim_products` 获取,不冗余进事实表——这是纯星型模型的做法。

### fct_orders(订单粒度)

订单事实表,每行一笔订单。订单级的商品汇总(商品数、总成本、总毛利)已由 `int_order_items_aggregated_to_order` 聚合好,这里只把它 JOIN 到订单自身的金额度量上组装——**没有 `group by`,也不依赖 `fct_order_items`**(消除了 fact→fact 依赖):

```sql
with orders as (
    select * from {{ ref('stg_orders') }}
),

item_rollup as (
    select * from {{ ref('int_order_items_aggregated_to_order') }}  -- 订单级汇总已在 int 算好
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.store_id,
        o.ordered_date,
        o.subtotal,
        o.tax_paid,
        o.order_total,
        coalesce(ir.item_count, 0)         as item_count,
        coalesce(ir.order_supply_cost, 0)  as order_supply_cost,
        coalesce(ir.order_gross_margin, 0) as order_gross_margin
    from orders o
    left join item_rollup ir on o.order_id = ir.order_id   -- 显式 ON,不用 USING!
)

select * from final
```

> **为什么不在这里 `group by fct_order_items`?** 事实表直接聚合另一张事实表会形成 fact→fact 依赖(坏味道)。把订单级汇总下沉到 `int_order_items_aggregated_to_order` 后,`fct_orders` 只依赖 int 层,自身回归「纯组装」职责:选外键、拼度量。

> `dim_products` / `dim_stores` 结构类似,篇幅所限不逐行展开:`dim_products` 把 `stg_products` LEFT JOIN `int_supplies_aggregated_per_product` 算出 `product_margin`;`dim_stores` 直接从 `stg_stores` 取门店属性。

## 数据流到这里闭环

```
stg_customers ──────────────────────────────────┐
                                                 ├─(ref)→ dim_customers
int_orders_aggregated_to_customer ←── stg_orders ┘

stg_orders ──────────────────────────────────────────┐
                                                      ├─(ref)→ fct_orders
int_order_items_aggregated_to_order ←── int_order_items_enriched ┘

stg_products ──(ref)→ dim_products
stg_stores  ───(ref)→ dim_stores
int_order_items_enriched ──(ref)→ fct_order_items
```

跑完 `dbt run`,MySQL `analytics` 库里就有了一套可供 BI 直接查询的星型模型(事实表 JOIN 维度表)。至此「从 RAW 到落库」的转换主链路完成,剩下的是校验(测试)、文档、上线。

## 执行

```bash
dbt run --select marts                # 只跑 marts 层
dbt run --select fct_orders           # 只跑单个 mart
dbt run --select +fct_orders          # 连同它的所有上游一起跑(+ 前缀 = 含上游)
```

`+fct_orders` 里的 `+` 是 dbt 的图选择器:表示「这个模型及其全部祖先」。这是靠 `ref()` 推导出来的血缘能力。

---

**上一章**:[06 · 第 4 步 intermediate 中间层](06-步骤4-intermediate中间层.md) · **下一章**:[08 · 第 6 步 测试与数据质量](08-步骤6-测试与数据质量.md)
