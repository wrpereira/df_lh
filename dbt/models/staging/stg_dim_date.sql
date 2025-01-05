-- {{ config(materialized="table") }}

-- with 
--     stg_sales_orderdetail as (
--         select
--             salesorderid_id
--             ,orderdate_ts
--             ,extract(day from orderdate_ts) as day_nr
--             ,extract(month from orderdate_ts) as month_nr
--             ,extract(year from orderdate_ts) as year_nr
--         from {{ ref('stg_sales_orderheader') }}
--     )

-- select
--     stg_sales_orderdetail.salesorderid_id
--     ,stg_sales_orderdetail.day_nr
--     ,stg_sales_orderdetail.month_nr
--     ,stg_sales_orderdetail.year_nr
-- from stg_sales_orderdetail
