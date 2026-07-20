-- staging: 清洗 raw_supplies
--   类型转换:字符串 'True'/'False' → 真正的 boolean(演示 staging 类型规整)
-- 对齐教学:docs/dbt-数仓教学/05-步骤3-staging清洗层.md
with source as (
    select * from {{ source('raw', 'raw_supplies') }}
),

renamed as (
    select
        id                                          as supply_id,
        sku                                         as product_sku,
        trim(name)                                  as supply_name,
        cost / 100.0                                as supply_cost,
        case when lower(perishable) = 'true' then true
             else false end                         as is_perishable
    from source
)

select * from renamed
