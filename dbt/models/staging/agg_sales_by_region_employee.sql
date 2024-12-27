{{ config(materialized='table') }}

with 
    sales_order_header as (
        select
            salesorderid
            ,salespersonid
            ,territoryid
            ,subtotal
            ,taxamt
            ,freight
        from {{ source('raw_data', 'sales_salesorderheader') }}
)
,sales_order_detail as (
    select
        salesorderid
        ,orderqty
        ,unitprice
    from {{ source('raw_data', 'sales_salesorderdetail') }}
)
,employee as (
    select
        businessentityid as employee_id
        ,firstname
        ,lastname
        ,jobtitle
    from {{ source('raw_data', 'humanresources_employee') }}
)
,store as (
    select
        businessentityid as store_id
        ,territoryid
        ,name as store_name
    from {{ source('raw_data', 'sales_store') }}
)
,final_aggregated_sales as (
    select
        sales_order_header.territoryid
        ,store.store_name
        ,sales_order_header.salespersonid as employee_id
        ,employee.firstname || ' ' || employee.lastname as employee_name
        ,employee.jobtitle
        ,sum(sales_order_detail.orderqty) as total_quantity_sold
        ,sum(sales_order_detail.unitprice * sales_order_detail.orderqty) as total_sales_value
        ,sum(sales_order_header.subtotal + sales_order_header.taxamt + sales_order_header.freight) as total_order_value
    from sales_order_header
    join sales_order_detail
        on sales_order_header.salesorderid = sales_order_detail.salesorderid
    left join employee
        on sales_order_header.salespersonid = employee.employee_id
    left join store
        on sales_order_header.territoryid = store.territoryid
    group by
        sales_order_header.territoryid
        ,store.store_name
        ,sales_order_header.salespersonid
        ,employee.firstname
        ,employee.lastname
        ,employee.jobtitle
)
select * 
from final_aggregated_sales

