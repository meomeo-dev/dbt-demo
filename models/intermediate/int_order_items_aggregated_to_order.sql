-- intermediate: 把订单行聚合到订单粒度(改变粒度:多条 order_item → 每订单一行)
--   模式 = "aggregate";命名遵循 int_[entity]s_[verb]s(order_items + aggregated)
--   从 int_order_items_enriched 聚合,而非 fct_order_items,以避免 fact→fact 依赖
-- 对齐教学:docs/dbt-数仓教学/06-步骤4-intermediate中间层.md
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
