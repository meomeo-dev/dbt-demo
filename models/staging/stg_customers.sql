-- staging layer: 清洗并标准化 raw_customers
with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

renamed as (
    select
        id                        as customer_id,
        name                      as customer_name,
        lower(trim(email))        as email,
        cast(created_at as date)  as created_date
    from source
)

select * from renamed
