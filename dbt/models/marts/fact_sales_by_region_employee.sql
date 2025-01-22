{{ config(materialized="table") }}

with 
    sales_salesorderheader as (
        select
            salesorderid_id
            , territoryid_id
            , cast(orderdate_dt as timestamp) as orderdate_ts 
        from {{ ref('stg_sales_salesorderheader') }}
    )

    , sales_salesorderdetail as (
        select
            salesorderid_id
            , productid_id
            , orderqty_qt
        from {{ ref('stg_sales_salesorderdetail') }}
    )

    , sales_salesperson as (
        select
            businessentityid_id as businessentityid_id_employee
            , territoryid_id
            , salesquota_nr
            , bonus_vr
            , commissionpct_vr
            , salesytd_vr
            , saleslastyear_vr
        from {{ ref('stg_sales_salesperson') }}
    )

    , historical_sales as (
        select
            sales_salesorderheader.territoryid_id
            , sales_salesorderdetail.productid_id
            , extract(year from sales_salesorderheader.orderdate_ts) as sale_year
            , extract(month from sales_salesorderheader.orderdate_ts) as sale_month
            , sum(sales_salesorderdetail.orderqty_qt) as total_quantity_sold
        from sales_salesorderheader
        join sales_salesorderdetail
            on sales_salesorderheader.salesorderid_id = sales_salesorderdetail.salesorderid_id
        group by 
            sales_salesorderheader.territoryid_id
            , sales_salesorderdetail.productid_id
            , sale_year
            , sale_month
    )

    , sales_with_person as (
        select
            historical_sales.territoryid_id
            , historical_sales.productid_id
            , historical_sales.sale_year
            , historical_sales.sale_month
            , historical_sales.total_quantity_sold
            , sales_salesperson.businessentityid_id_employee
            , sales_salesperson.salesquota_nr
            , sales_salesperson.bonus_vr
            , sales_salesperson.commissionpct_vr
            , sales_salesperson.salesytd_vr
            , sales_salesperson.saleslastyear_vr
        from historical_sales
        left join sales_salesperson
            on historical_sales.territoryid_id = sales_salesperson.territoryid_id
    )

select *
from sales_with_person
