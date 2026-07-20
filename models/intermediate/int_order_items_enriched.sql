-- intermediate: 把订单行打平,补齐产品售价与供应成本(模式 = "join")
--   staging 不许 JOIN,跨表拼接的活儿落在 intermediate 层
-- 对齐教学:docs/dbt-数仓教学/06-步骤4-intermediate中间层.md
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
        p.product_price,                                       -- 该行售价
        coalesce(sc.total_supply_cost, 0)   as supply_cost,    -- 该行成本
        p.product_price
            - coalesce(sc.total_supply_cost, 0) as gross_margin -- 毛利 = 售价 - 成本
    from order_items oi
    left join products     p  on oi.product_sku = p.product_sku
    left join supply_costs sc on oi.product_sku = sc.product_sku
)

select * from joined
