{{
  config(
    materialized = 'incremental',
    partition_by = {
        'field': 'UpdatedAt', 
        'data_type': 'timestamp'
    },
    incremental_strategy = 'insert_overwrite',
    enabled = false
  )
}}

with get_orders_revenue as(
  select
    pro.OrderId,
    sum(pro.Price) as Revenue
  from {{ ref('int_orders_items_products_joined') }} as pro
  group by 1
)

select
  ord.OrderId,
  ord.OrderPlacedTimestamp,
  ord.UpdatedAt,
  ord.OrderStatus,
  ord.SalesPerson,
  rev.Revenue
from get_orders_revenue as rev
join {{ ref('stg_furniture_mart_orders') }} as ord
  on rev.OrderId = ord.OrderId
{% if is_incremental() %}
where date(ord.UpdatedAt) >= date_sub(date(_dbt_max_partition), interval 2 day)
{% endif %}