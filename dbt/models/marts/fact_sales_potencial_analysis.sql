{{ config(materialized='table') }}

with 
    sales_salesorderheader as (
        select
             salesorderid_id
            ,territoryid_id
            ,orderdate_dt
            ,totaldue_vr
        from {{ ref('stg_sales_salesorderheader') }}
    ),

    sales_salesterritory as (
        select
             territoryid_id
            ,territory_nm
            ,salesytd_vr * 1000000 as salesytd_vr
            ,saleslastyear_vr * 1000000 as saleslastyear_vr
            ,countryregioncode_cd
        from {{ ref('stg_sales_salesterritory') }}
    ),

    sales_with_territory as (
        select
             sales_salesorderheader.territoryid_id
            ,sales_salesterritory.territory_nm
            ,sales_salesterritory.salesytd_vr
            ,sales_salesterritory.saleslastyear_vr
            ,sales_salesterritory.countryregioncode_cd
            ,round(sum(sales_salesorderheader.totaldue_vr), 2) as total_sales_value
            ,count(sales_salesorderheader.salesorderid_id) as total_orders
            ,min(sales_salesorderheader.orderdate_dt) as first_order_date
            ,max(sales_salesorderheader.orderdate_dt) as last_order_date
        from sales_salesorderheader
        left join sales_salesterritory
            on sales_salesorderheader.territoryid_id = sales_salesterritory.territoryid_id
        group by
             sales_salesorderheader.territoryid_id
            ,sales_salesterritory.territory_nm
            ,sales_salesterritory.salesytd_vr
            ,sales_salesterritory.saleslastyear_vr
            ,sales_salesterritory.countryregioncode_cd
    ),

    final_sales_analysis as (
        select
             sales_with_territory.territoryid_id
            ,sales_with_territory.territory_nm
            ,sales_with_territory.countryregioncode_cd
            ,round(sales_with_territory.salesytd_vr, 2) as salesytd_vr
            ,round(sales_with_territory.saleslastyear_vr, 2) as saleslastyear_vr
            ,sales_with_territory.total_sales_value
            ,sales_with_territory.total_orders
            ,sales_with_territory.first_order_date
            ,sales_with_territory.last_order_date
            ,extract(year from sales_with_territory.first_order_date) as first_order_year
            ,extract(year from sales_with_territory.last_order_date) as last_order_year
        from sales_with_territory
    )

select * 
from final_sales_analysis
