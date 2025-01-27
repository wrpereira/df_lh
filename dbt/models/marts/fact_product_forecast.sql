{{ config(materialized="table") }}

with 
    sales_salesorderheader as (
        select
            salesorderid_id,
            territoryid_id as store_id,
            orderdate_dt
        from {{ ref('stg_sales_salesorderheader') }}
    ),

    sales_salesorderdetail as (
        select
            salesorderid_id,
            productid_id,
            orderqty_qt,
            unitprice_vr
        from {{ ref('stg_sales_salesorderdetail') }}
    ),

    historical_sales as (
        select
            sales_salesorderheader.store_id,
            sales_salesorderdetail.productid_id,
            cast(extract(year from sales_salesorderheader.orderdate_dt) as string) || '-' || 
            lpad(cast(extract(month from sales_salesorderheader.orderdate_dt) as string), 2, '0') as sale_month,
            sum(sales_salesorderdetail.orderqty_qt) as total_quantity_sold,
            avg(sales_salesorderdetail.unitprice_vr) as avg_unit_price
        from sales_salesorderheader
        join sales_salesorderdetail
            on sales_salesorderheader.salesorderid_id = sales_salesorderdetail.salesorderid_id
        where sales_salesorderheader.orderdate_dt >= '2011-01-01'  -- Ajustando para as vendas de 2011
        and sales_salesorderheader.orderdate_dt <= '2014-12-31'  -- Ajustando para as vendas até 2014
        group by 
            sales_salesorderheader.store_id,
            sales_salesorderdetail.productid_id,
            sale_month
    ),

    forecast_dates as (
        select 
            date as forecast_date
        from unnest(generate_date_array(current_date, date_add(current_date, interval 3 month), interval 1 month)) as date
    ),

    fact_product_forecast as (
        select
            historical_sales.store_id,
            historical_sales.productid_id,
            forecast_dates.forecast_date,
            round(sum(historical_sales.total_quantity_sold) over (partition by historical_sales.store_id, historical_sales.productid_id), 2) as forecast_quantity,
            round(avg(historical_sales.avg_unit_price) over (partition by historical_sales.productid_id), 2) as avg_unit_price,
            round(sum(historical_sales.total_quantity_sold) over (partition by historical_sales.store_id, historical_sales.productid_id) * 
            avg(historical_sales.avg_unit_price) over (partition by historical_sales.productid_id), 2) as forecast_sales_value
        from historical_sales
        cross join forecast_dates
    )

select *
from fact_product_forecast
