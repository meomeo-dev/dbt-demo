-- staging: 清洗 raw_orders
--   重命名 + 类型规整 + 基础计算(金额"分转元",属官方允许的 staging 轻度转换)
-- 对齐教学:docs/dbt-数仓教学/05-步骤3-staging清洗层.md
with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

renamed as (
    select
        id                              as order_id,
        customer                        as customer_id,   -- 重命名为统一的外键名
        store_id,
        cast(ordered_at as date)        as ordered_date,  -- datetime → date
        subtotal    / 100.0             as subtotal,      -- 分 → 元
        tax_paid    / 100.0             as tax_paid,
        order_total / 100.0             as order_total
    from source
)

select * from renamed
