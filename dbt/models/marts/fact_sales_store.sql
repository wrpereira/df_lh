{{ config(materialized='table') }}

with 
    fact_sales_store as (
        select
            sales_orderheader.salesorderid_id
            , sales_store.businessentityid_id as store_id        
            , sales_store.store_nm
            , sales_orderdetail.productid_id
            , sales_orderdetail.orderqty_qt   
            , sales_orderdetail.unitprice_vr                     
            , (sales_orderdetail.unitprice_vr * sales_orderdetail.orderqty_qt) as sales_value
            , (sales_orderheader.subtotal_vr + sales_orderheader.taxamt_vr + sales_orderheader.freight_vr) as order_value
            , sales_orderheader.orderdate_dt
            , sales_orderheader.shipdate_dt
        from {{ ref('stg_sales_store') }} sales_store
        join {{ ref('stg_sales_customer') }} sales_customer
            on sales_store.businessentityid_id = sales_customer.storeid_id
        join {{ ref('stg_sales_salesorderheader') }} sales_orderheader
            on sales_customer.customerid_id = sales_orderheader.customerid_id
        join {{ ref('stg_sales_salesorderdetail') }} sales_orderdetail
            on sales_orderheader.salesorderid_id = sales_orderdetail.salesorderid_id
    )

select *
from fact_sales_store
