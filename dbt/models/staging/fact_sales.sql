{{ config(materialized='table') }}

with 
    sales_order_header as ( 
        select
            salesorderid_id
            ,customerid_id
            ,salespersonid_id
            ,orderdate_ts
            ,shipdate_ts
            ,territoryid_id
            ,subtotal_vr
            ,taxamt_vr
            ,freight_vr
        from {{ ref('stg_sales_orderheader') }}
),

    sales_order_detail as (
        select
            salesorderid_id
            ,productid_id
            ,orderqty_qt
            ,unitprice_vr
            ,unitpricediscount_vr
        from {{ ref('stg_sales_orderdetail') }}
),

    product as (
        select
            productid_id
            ,name_nm as product_name
            ,productnumber_cd
        from {{ ref('stg_production_product') }}
),

    store as (
        select
            businessentityid_id as store_id
            ,name_nm as store_name
            ,territoryid_id
        from {{ ref('stg_sales_store') }}
),

final_fact_sales as (
    select
        sales_order_header.salesorderid_id as salesorderid
        ,sales_order_header.customerid_id as customerid
        ,sales_order_header.salespersonid_id as salespersonid
        ,sales_order_header.orderdate_ts as orderdate
        ,sales_order_header.shipdate_ts as shipdate
        ,sales_order_detail.productid_id as productid
        ,product.product_name
        ,product.productnumber_cd as productnumber
        ,store.store_id
        ,store.store_name
        ,sales_order_header.territoryid_id as territoryid
        ,sum(sales_order_detail.orderqty_qt) as total_quantity
        ,sum(sales_order_detail.unitprice_vr * sales_order_detail.orderqty_qt) as total_sales_value
        ,sum(sales_order_header.subtotal_vr + sales_order_header.taxamt_vr + sales_order_header.freight_vr) as total_order_value
    from sales_order_header
    join sales_order_detail
        on sales_order_header.salesorderid_id = sales_order_detail.salesorderid_id
    left join product
        on sales_order_detail.productid_id = product.productid_id
    left join store
        on sales_order_header.territoryid_id = store.territoryid_id
    group by
        sales_order_header.salesorderid_id
        ,sales_order_header.customerid_id
        ,sales_order_header.salespersonid_id
        ,sales_order_header.orderdate_ts
        ,sales_order_header.shipdate_ts
        ,sales_order_detail.productid_id
        ,product.product_name
        ,product.productnumber_cd
        ,store.store_id
        ,store.store_name
        ,sales_order_header.territoryid_id
)

select * 
from final_fact_sales
