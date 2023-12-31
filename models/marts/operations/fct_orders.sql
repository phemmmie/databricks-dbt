{{
  config(
    materialized = 'table'
  )
}}


select
  ord.OrderId,
  ord.CustomerId,
  cus.Name,
  ord.SalesPerson,  
  ord.OrderStatus,
  ord.OrderPlacedTimestamp,
  1 as Order
from {{ ref('stg_furniture_mart_orders') }} as ord

left join {{ ref('customers_snapshot_timestamp_strategy') }} as cus
  on ord.CustomerId = cus.CustomerId
-- uncomment the join criteria below to implement a time range join. 
--   and ord.OrderPlacedTimestamp between cus.dbt_valid_from and ifnull(cus.dbt_valid_to, '2099-12-31'::timestamp_ntz)

-- filters the snapshot table to only include the active records.
-- where cus.dbt_valid_to is null 