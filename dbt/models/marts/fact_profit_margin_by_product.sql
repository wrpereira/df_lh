{{ config(materialized='table') }}

with 
    sales_salesorderdetail as (
        select
            sales_salesorderdetail.productid_id
            , sum(sales_salesorderdetail.orderqty_qt * (sales_salesorderdetail.unitprice_vr - sales_salesorderdetail.unitpricediscount_vr)) as total_revenue
        from {{ ref('stg_sales_salesorderdetail') }} as sales_salesorderdetail
        group by 
            sales_salesorderdetail.productid_id
    )

    , production_productcosthistory as (
        select
            production_productcosthistory.productid_id
            , avg(production_productcosthistory.standardcost_vr) as avg_cost
        from {{ ref('stg_production_productcosthistory') }} as production_productcosthistory
        group by 
            production_productcosthistory.productid_id
    )

    , production_product as (
        select
            production_product.productid_id
            , production_product.product_nm
            , production_product.listprice_vr
        from {{ ref('stg_production_product') }} as production_product
    )

    , fact_profit_margin_by_product as (
        select
            sales_salesorderdetail.productid_id
            , production_product.product_nm
            , round(sales_salesorderdetail.total_revenue, 2) as total_revenue
            , round(production_productcosthistory.avg_cost, 2) as avg_cost
            , round(
                sales_salesorderdetail.total_revenue - 
                (sales_salesorderdetail.total_revenue / production_product.listprice_vr) * production_productcosthistory.avg_cost, 
                2
              ) as total_profit
            , round(
                (sales_salesorderdetail.total_revenue - 
                (sales_salesorderdetail.total_revenue / production_product.listprice_vr) * production_productcosthistory.avg_cost) / 
                sales_salesorderdetail.total_revenue * 100, 
                2
              ) as profit_margin_percentage
        from sales_salesorderdetail
        left join production_productcosthistory
            on sales_salesorderdetail.productid_id = production_productcosthistory.productid_id
        left join production_product
            on sales_salesorderdetail.productid_id = production_product.productid_id
    )

select *
from fact_profit_margin_by_product
