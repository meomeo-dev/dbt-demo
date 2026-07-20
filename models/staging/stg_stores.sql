-- staging: 清洗 raw_stores
-- 对齐教学:docs/dbt-数仓教学/05-步骤3-staging清洗层.md
with source as (
    select * from {{ source('raw', 'raw_stores') }}
),

renamed as (
    select
        id                          as store_id,
        trim(name)                  as store_name,
        cast(opened_at as date)     as opened_date,
        tax_rate
    from source
)

select * from renamed
