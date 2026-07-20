-- intermediate: 按产品聚合供应成本(改变粒度:多条 supplies → 每产品一行)
--   模式 = "aggregate";命名遵循官方约定 int_[entity]s_[verb]s(supplies + aggregated)
-- 对齐教学:docs/dbt-数仓教学/06-步骤4-intermediate中间层.md
with supplies as (
    select * from {{ ref('stg_supplies') }}
),

aggregated as (
    select
        product_sku,
        count(*)            as supply_count,       -- 该产品用到的供应品种类数
        sum(supply_cost)    as total_supply_cost   -- 制作该产品的总耗材成本
    from supplies
    group by product_sku
)

select * from aggregated
