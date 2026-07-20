-- marts / 事实表: 订单粒度(每行一笔订单)
--   纯组装:外键指向 dim_customers/dim_stores,度量来自 stg_orders(金额)
--   与订单行汇总度量(来自 int 层,不在此聚合,也不依赖 fct_order_items)
-- 对齐教学:docs/dbt-数仓教学/07-步骤5-marts业务聚合层.md
with orders as (
    select * from {{ ref('stg_orders') }}
),

item_rollup as (
    select * from {{ ref('int_order_items_aggregated_to_order') }}
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
    left join item_rollup ir on o.order_id = ir.order_id   -- 显式 ON,不用 USING
)

select * from final
