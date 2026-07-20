-- marts / 维度表: 产品维度,含售价、成本、毛利
--   描述"卖的什么"
-- 对齐教学:docs/dbt-数仓教学/07-步骤5-marts业务聚合层.md
with products as (
    select * from {{ ref('stg_products') }}
),

supply_costs as (
    select * from {{ ref('int_supplies_aggregated_per_product') }}
),

final as (
    select
        p.product_sku,
        p.product_name,
        p.product_type,
        p.is_food_item,
        p.product_price,
        coalesce(sc.total_supply_cost, 0)               as supply_cost,
        p.product_price - coalesce(sc.total_supply_cost, 0) as product_margin
    from products p
    left join supply_costs sc on p.product_sku = sc.product_sku
)

select * from final
