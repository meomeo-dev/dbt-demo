-- staging: 清洗 raw_products
--   分类归桶(is_food_item)属官方允许的 staging 转换
-- 对齐教学:docs/dbt-数仓教学/05-步骤3-staging清洗层.md
with source as (
    select * from {{ source('raw', 'raw_products') }}
),

renamed as (
    select
        sku                                        as product_sku,
        name                                       as product_name,
        type                                       as product_type,
        description                                as product_description,
        price / 100.0                              as product_price,
        case when type = 'jaffle' then true
             else false end                        as is_food_item
    from source
)

select * from renamed
