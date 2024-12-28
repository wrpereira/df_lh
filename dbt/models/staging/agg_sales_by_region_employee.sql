{{ config(materialized='table') }}

with 
    sales_order_header as (
        select
            salesorderid_id
            ,salespersonid_id
            ,territoryid_id
            ,subtotal_vr
            ,taxamt_vr
            ,freight_vr
        from {{ ref('stg_sales_orderheader') }}
)
,sales_order_detail as (
    select
        salesorderid_id
        ,orderqty_qt
        ,unitprice_vr
    from {{ ref('stg_sales_orderdetail') }}
)
,employee as (
    select
        businessentityid_id as employee_id
        ,firstname
        ,lastname
        ,jobtitle
    from {{ ref('stg_humanresources_employee') }}
)
,store as (
    select
        businessentityid_id as store_id
        ,territoryid_id
        ,name_nm as store_name
    from {{ ref('stg_sales_store') }}
)
,final_aggregated_sales as (
    select
        sales_order_header.territoryid_id as territoryid
        ,store.store_name
        ,sales_order_header.salespersonid_id as employee_id
        ,employee.firstname || ' ' || employee.lastname as employee_name
        ,employee.jobtitle
        ,sum(sales_order_detail.orderqty_qt) as total_quantity_sold
        ,sum(sales_order_detail.unitprice_vr * sales_order_detail.orderqty_qt) as total_sales_value
        ,sum(sales_order_header.subtotal_vr + sales_order_header.taxamt_vr + sales_order_header.freight_vr) as total_order_value
    from sales_order_header
    join sales_order_detail
        on sales_order_header.salesorderid_id = sales_order_detail.salesorderid_id
    left join employee
        on sales_order_header.salespersonid_id = employee.employee_id
    left join store
        on sales_order_header.territoryid_id = store.territoryid_id
    group by
        sales_order_header.territoryid_id
        ,store.store_name
        ,sales_order_header.salespersonid_id
        ,employee.firstname
        ,employee.lastname
        ,employee.jobtitle
)
select * 
from final_aggregated_sales


