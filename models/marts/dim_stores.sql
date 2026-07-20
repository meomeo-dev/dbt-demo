-- marts / 维度表: 门店维度
--   描述"在哪家店"
-- 对齐教学:docs/dbt-数仓教学/07-步骤5-marts业务聚合层.md
with stores as (
    select * from {{ ref('stg_stores') }}
)

select
    store_id,
    store_name,
    opened_date,
    tax_rate
from stores
