-- staging: 清洗 raw_customers(一对一映射,只重命名,不 JOIN 不聚合)
-- 对齐教学:docs/dbt-数仓教学/05-步骤3-staging清洗层.md
with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

renamed as (
    select
        id           as customer_id,
        trim(name)   as customer_name
    from source
)

select * from renamed
