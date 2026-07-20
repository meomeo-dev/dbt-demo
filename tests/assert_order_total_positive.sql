-- singular test(单点测试):订单总额不应为负
--   返回任意行即判定失败(返回 0 行 = 通过)
-- 对齐教学:docs/dbt-数仓教学/08-步骤6-测试与数据质量.md
select
    order_id,
    order_total
from {{ ref('fct_orders') }}
where order_total < 0
