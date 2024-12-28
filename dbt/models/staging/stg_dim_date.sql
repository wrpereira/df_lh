{{ config(materialized="table") }}

with 
    stg_sales_orderdetail as (
        select
            salesorderid_id
            ,order_date_dt
            ,extract(day from order_date_dt) as day_nr
            ,extract(month from order_date_dt) as month_nr
            ,extract(year from order_date_dt) as year_nr
        from {{ ref('stg_sales_orderdetail') }}
    )

select
    stg_sales_orderdetail.salesorderid_id
    ,stg_sales_orderdetail.day_nr
    ,stg_sales_orderdetail.month_nr
    ,stg_sales_orderdetail.year_nr
from stg_sales_orderdetail
