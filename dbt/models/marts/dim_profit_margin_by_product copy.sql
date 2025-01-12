{{ config(materialized='table') }}

with 
    sales_data as (
        select
             sod.productid_id
            ,sum(sod.orderqty_qt * (sod.unitprice_vr - sod.unitpricediscount_vr)) as total_revenue
        from {{ ref('stg_sales_salesorderdetail') }} sod
        group by sod.productid_id
    ),

    cost_data as (
        select
             pch.productid_id
            ,avg(pch.standardcost_vr) as avg_cost
        from {{ ref('stg_production_productcosthistory') }} pch
        group by pch.productid_id
    ),

    product_info as (
        select
             prod.productid_id
            ,prod.product_nm
            ,prod.listprice_vr
        from {{ ref('stg_production_product') }} prod
    ),

    profit_calculation as (
        select
             sales.productid_id
            ,prod.product_nm
            ,sales.total_revenue
            ,cost.avg_cost
            ,(sales.total_revenue - (sales.total_revenue / prod.listprice_vr) * cost.avg_cost) as total_profit
            ,(sales.total_revenue - (sales.total_revenue / prod.listprice_vr) * cost.avg_cost) / sales.total_revenue * 100 as profit_margin_percentage
        from sales_data sales
        left join cost_data cost
            on sales.productid_id = cost.productid_id
        left join product_info prod
            on sales.productid_id = prod.productid_id
    )

select *
from profit_calculation
