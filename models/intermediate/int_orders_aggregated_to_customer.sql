-- intermediate: 把订单聚合到客户粒度(改变粒度:多笔订单 → 每客户一行)
--   模式 = "aggregate";命名遵循 int_[entity]s_[verb]s(orders + aggregated)
--   产出客户级累计指标,供 dim_customers 组装为富维度(customer-360)
-- 对齐教学:docs/dbt-数仓教学/06-步骤4-intermediate中间层.md
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
