{{ config(materialized="table") }}

with 
    sales_salesorderheader as (
        select
             salesorderid_id
            ,territoryid_id
            ,cast(orderdate_dt as timestamp) as orderdate_ts 
        from {{ ref('stg_sales_salesorderheader') }}
    ),

    sales_salesorderdetail as (
        select
             salesorderid_id
            ,productid_id
            ,orderqty_qt
        from {{ ref('stg_sales_salesorderdetail') }}
    ),

    historical_sales as (
        select
             sales_salesorderheader.territoryid_id
            ,sales_salesorderdetail.productid_id
            ,extract(year from sales_salesorderheader.orderdate_ts) as sale_year
            ,extract(month from sales_salesorderheader.orderdate_ts) as sale_month
            ,sum(sales_salesorderdetail.orderqty_qt) as total_quantity_sold
        from sales_salesorderheader
        join sales_salesorderdetail
             on sales_salesorderheader.salesorderid_id = sales_salesorderdetail.salesorderid_id
        group by 
             sales_salesorderheader.territoryid_id
            ,sales_salesorderdetail.productid_id
            ,sale_year
            ,sale_month
    )

select *
from historical_sales
