{{ config(materialized='table') }}

with 
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
        from {{ ref('stg_sales_store') }}
    ),
    forecast_data as (
        select
            productid
            ,store_id
            ,forecast_date
            ,forecast_quantity
        from {{ ref('mock_product_forecast') }}
    ),
    sales_order_header as (
        select
            salesorderid_id
            ,territoryid_id
            ,salespersonid_id
            ,subtotal_vr
            ,taxamt_vr
            ,freight_vr
        from {{ ref('stg_sales_orderheader') }}
    ),
    sales_order_detail as (
        select
            salesorderid_id
            ,orderqty_qt
            ,unitprice_vr
        from {{ ref('stg_sales_orderdetail') }}
    ),
    final_aggregated_sales as (
        select
            sales_order_header.territoryid_id as territoryid
            ,store.store_name
            ,sales_order_header.salespersonid_id as employee_id
            ,stg_person_person.firstname_nm || ' ' || stg_person_person.lastname_nm as employee_name
            ,stg_humanresources_employee.jobtitle_nm as jobtitle
            ,sum(sales_order_detail.orderqty_qt) as total_quantity_sold
            ,sum(sales_order_detail.unitprice_vr * sales_order_detail.orderqty_qt) as total_sales_value
            ,sum(sales_order_header.subtotal_vr + sales_order_header.taxamt_vr + sales_order_header.freight_vr) as total_order_value
        from sales_order_header
        join sales_order_detail
            on sales_order_header.salesorderid_id = sales_order_detail.salesorderid_id
        left join {{ ref('stg_humanresources_employee') }}
            on sales_order_header.salespersonid_id = stg_humanresources_employee.businessentityid_id
        left join {{ ref('stg_person_person') }}
            on stg_humanresources_employee.businessentityid_id = stg_person_person.businessentityid_id
        left join store
            on sales_order_header.territoryid_id = store.store_id
        group by
            sales_order_header.territoryid_id
            ,store.store_name
            ,sales_order_header.salespersonid_id
            ,stg_person_person.firstname_nm
            ,stg_person_person.lastname_nm
            ,stg_humanresources_employee.jobtitle_nm
    )

select * 
from final_aggregated_sales
