-- marts / 维度表: 客户维度,含首末单日期与累计订单指标
--   星型模型的 dim_ 表:描述"谁";累计指标由 int 层聚合好,这里只做组装
-- 对齐教学:docs/dbt-数仓教学/07-步骤5-marts业务聚合层.md、00-数仓基础(Kimball)
with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select * from {{ ref('int_orders_aggregated_to_customer') }}
),

final as (
    select
        c.customer_id,
        c.customer_name,
        coalesce(co.lifetime_orders, 0)   as lifetime_orders,
        coalesce(co.lifetime_spend, 0)    as lifetime_spend,
        co.first_order_date,
        co.most_recent_order_date,
        case when co.lifetime_orders is null then false
             else true end               as is_active_customer
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id   -- 显式 ON,不用 USING
)

select * from final
