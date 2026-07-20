-- marts / 事实表: 订单行粒度(最细粒度事实表,每行一个下单商品)
--   纯星型(pure star schema):只保留外键 + 度量;
--   描述性属性(product_name/product_type 等)一律靠 JOIN dim_products 获取,不冗余进事实表。
-- 对齐教学:docs/dbt-数仓教学/07-步骤5-marts业务聚合层.md、00-数仓基础(星型模型)
with order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select
        oi.order_item_id,          -- 主键
        oi.order_id,               -- 外键 → fct_orders
        o.customer_id,             -- 退化维度(degenerate dimension)
        o.store_id,                -- 退化维度
        o.ordered_date,            -- 退化维度
        oi.product_sku,            -- 外键 → dim_products
        oi.product_price,          -- 度量:售价
        oi.supply_cost,            -- 度量:成本
        oi.gross_margin            -- 度量:毛利
    from order_items oi
    left join orders o on oi.order_id = o.order_id
)

select * from final
