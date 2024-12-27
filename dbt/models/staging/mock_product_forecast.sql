{{ config(materialized='table') }}

with product_forecast as (
    select 1 as productid, 101 as store_id, '2024-01-01' as forecast_date, 200 as forecast_quantity
    union all
    select 2, 102, '2024-01-01', 150
    union all
    select 3, 103, '2024-01-02', 180
    union all
    select 4, 104, '2024-01-02', 220
)

select *
from product_forecast
