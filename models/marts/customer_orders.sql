-- marts layer: 汇总每个客户的订单指标 (materialized as table)
with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

order_summary as (
    select
        customer_id,
        count(*)                                            as total_orders,
        sum(amount)                                         as total_spent,
        sum(case when status = 'completed' then amount
                 else 0 end)                               as completed_amount,
        min(order_date)                                     as first_order_date,
        max(order_date)                                     as last_order_date
    from orders
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.created_date,
        coalesce(o.total_orders, 0)      as total_orders,
        coalesce(o.total_spent, 0)       as total_spent,
        coalesce(o.completed_amount, 0)  as completed_amount,
        o.first_order_date,
        o.last_order_date
    from customers c
    left join order_summary o on c.customer_id = o.customer_id
)

select * from final
