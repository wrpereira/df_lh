{{ config(materialized='table') }}

with 
    sales_order_header as ( 
        select
            salesorderid
            ,customerid
            ,salespersonid
            ,orderdate
            ,shipdate
            ,territoryid
            ,subtotal
            ,taxamt
            ,freight
        from {{ source('raw_data', 'sales_salesorderheader') }}
),

sales_order_detail as (
        select
            salesorderid
            ,productid
            ,orderqty
            ,unitprice
            ,unitpricediscount
        from {{ source('raw_data', 'sales_salesorderdetail') }}
),

product as (
        select
            productid
            ,name as product_name
            ,productnumber
        from {{ source('raw_data', 'production_product') }}
),

store as (
        select
            businessentityid as store_id
            ,name as store_name
            ,territoryid
        from {{ source('raw_data', 'sales_store') }}
),

final_fact_sales as (
        select
            sales_order_header.salesorderid
            ,sales_order_header.customerid
            ,sales_order_header.salespersonid
            ,sales_order_header.orderdate
            ,sales_order_header.shipdate
            ,sales_order_detail.productid
            ,product.name as product_name
            ,product.productnumber
            ,store.businessentityid as store_id
            ,store.name as store_name
            ,sales_order_header.territoryid
            ,sum(sales_order_detail.orderqty) as total_quantity
            ,sum(sales_order_detail.unitprice * sales_order_detail.orderqty) as total_sales_value
            ,sum(sales_order_header.subtotal + sales_order_header.taxamt + sales_order_header.freight) as total_order_value
        from sales_order_header
        join sales_order_detail
            on sales_order_header.salesorderid = sales_order_detail.salesorderid
        left join product
            on sales_order_detail.productid = product.productid
        left join store
            on sales_order_header.territoryid = store.territoryid
        group by
            sales_order_header.salesorderid
            ,sales_order_header.customerid
            ,sales_order_header.salespersonid
            ,sales_order_header.orderdate
            ,sales_order_header.shipdate
            ,sales_order_detail.productid
            ,product.name
            ,product.productnumber
            ,store.businessentityid
            ,store.name
            ,sales_order_header.territoryid
)

select * 
from final_fact_sales;
