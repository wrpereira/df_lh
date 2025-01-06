{{ config(materialized='table') }}

with 
    sales_salesorderheader as ( 
        select
             salesorderid_id
            ,customerid_id
            ,orderdate_dt
            ,shipdate_dt
            ,territoryid_id
            ,subtotal_vr
            ,taxamt_vr
            ,freight_vr
        from {{ ref('stg_sales_salesorderheader') }}
    ),

    sales_salesorderdetail as (
        select
             salesorderid_id
            ,productid_id
            ,orderqty_qt
            ,unitprice_vr
            ,unitpricediscount_vr
        from {{ ref('stg_sales_salesorderdetail') }}
    ),

    production_product as (
        select
             productid_id
            ,product_nm
        from {{ ref('stg_production_product') }}
    ),

    sales_store as (
        select
             businessentityid_id
            ,store_nm
        from {{ ref('stg_sales_store') }}
    ),

    sales_salesterritory as (
        select
             territoryid_id
            ,territory_nm
        from {{ ref('stg_sales_salesterritory') }}
    ),

    sales_store_sales_salesterritory as (
        select
             sales_store.businessentityid_id
            ,sales_store.store_nm
            ,sales_salesterritory.territoryid_id
            ,sales_salesterritory.territory_nm
        from {{ ref('stg_sales_store') }} sales_store
        left join {{ ref('stg_sales_salesterritory') }} sales_salesterritory
             on sales_store.businessentityid_id = sales_salesterritory.territoryid_id    
    ),    

    final_fact_sales as (
        select
             sales_salesorderheader.customerid_id
            ,sales_salesorderheader.salesorderid_id 
            ,production_product.product_nm   
            ,sum(sales_salesorderdetail.orderqty_qt) as total_quantity
            ,sum(sales_salesorderdetail.unitprice_vr * sales_salesorderdetail.orderqty_qt) as total_sales_value
            ,sum(sales_salesorderheader.subtotal_vr + sales_salesorderheader.taxamt_vr + sales_salesorderheader.freight_vr) as total_order_value
            ,sales_salesorderheader.orderdate_dt
            ,sales_salesorderheader.shipdate_dt
            ,sales_store_sales_salesterritory.store_nm
            ,sales_store_sales_salesterritory.territory_nm      
        from sales_salesorderheader
        join sales_salesorderdetail
             on sales_salesorderheader.salesorderid_id = sales_salesorderdetail.salesorderid_id
        left join production_product
             on sales_salesorderdetail.productid_id = production_product.productid_id
        left join sales_store_sales_salesterritory
             on sales_salesorderheader.territoryid_id = sales_store_sales_salesterritory.territoryid_id
        group by
             sales_salesorderheader.customerid_id
            ,sales_salesorderheader.salesorderid_id 
            ,production_product.product_nm      
            ,sales_salesorderheader.orderdate_dt
            ,sales_salesorderheader.shipdate_dt
            ,sales_store_sales_salesterritory.store_nm
            ,sales_store_sales_salesterritory.territory_nm 
    )

select * 
from final_fact_sales
