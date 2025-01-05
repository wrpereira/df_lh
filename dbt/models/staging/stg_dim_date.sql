{{ config(materialized="table") }}

with 
    sales_salesorderheader as (
        select
             salesorderid_id
            ,extract(day from orderdate_dt) as day_nr
            ,extract(month from orderdate_dt) as month_nr
            ,extract(year from orderdate_dt) as year_nr
        from {{ ref('stg_sales_salesorderheader') }}
    ),

    final_dim_date as (
        select
             sales_salesorderheader.salesorderid_id
            ,sales_salesorderheader.day_nr
            ,sales_salesorderheader.month_nr
            ,sales_salesorderheader.year_nr
        from sales_salesorderheader
)

select*
from final_dim_date
