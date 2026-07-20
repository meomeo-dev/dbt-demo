-- staging: 清洗 raw_items(订单行)
-- 对齐教学:docs/dbt-数仓教学/05-步骤3-staging清洗层.md
with source as (
    select * from {{ source('raw', 'raw_items') }}
),

renamed as (
    select
        id        as order_item_id,
        order_id,
        sku       as product_sku
    from source
)

select * from renamed
