-- staging layer: 清洗并标准化 raw_orders
with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

renamed as (
    select
        id                          as order_id,
        customer_id,
        cast(amount as decimal(10,2)) as amount,
        status,
        cast(ordered_at as date)    as order_date
    from source
)

select * from renamed
