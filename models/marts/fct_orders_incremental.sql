-- marts / 事实表(增量版): 演示 incremental 物化的 append 策略
--
--   ⚠️ 平台约束(实测结论,见 docs/dbt-数仓教学/10 章):
--   Trino MySQL connector 不支持 row-level DELETE / 事务性 MERGE,
--   因此 incremental_strategy 只能用 'append'(纯 INSERT 追加);
--   'merge' 与 'delete+insert' 会报 "does not support MERGE with transactional execution"。
--   append 无法更新/去重已有行,仅适合"只增不改"的事件型数据(如订单流水)。
--
--   views_enabled=false:强制增量的临时中间关系用 table 而非 view
--   (Trino MySQL connector 亦不支持 CREATE VIEW)。
--
-- 对齐教学:docs/dbt-数仓教学/10-步骤8-物化策略与增量.md
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        views_enabled=false
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    customer_id,
    store_id,
    ordered_date,
    subtotal,
    tax_paid,
    order_total
from orders

{% if is_incremental() %}
    -- 仅在增量运行时执行:只追加比"已加载数据里最新日期"更新的订单
    where ordered_date > (select max(ordered_date) from {{ this }})
{% endif %}
