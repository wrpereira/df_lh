{{ config(materialized='table') }}

with 
    production_product as (
        select
             productid_id
            ,product_nm
            ,productnumber_cd
        from {{ ref('stg_production_product') }}
    ),

    sales_store as (
        select
             businessentityid_id as businessentityid_id_store
            ,store_nm
        from {{ ref('stg_sales_store') }}
    ),

    forecast_data as (
        select
             productid_id
            ,store_id
            ,forecast_quantity
        from {{ ref('mock_product_forecast') }}
    ),

    sales_salesorderheader as (
        select
             salesorderid_id
            ,territoryid_id
            ,salespersonid_id
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
        from {{ ref('stg_sales_salesorderdetail') }}
    ),

    final_fact_forecast as (
        select
             sales_salesorderheader.territoryid_id
            ,sales_store.store_nm
            ,sales_salesorderheader.salespersonid_id as employee_id
            ,stg_person_person.firstname_nm || ' ' || stg_person_person.lastname_nm as employee_name
            ,stg_humanresources_employee.jobtitle_nm
            ,forecast_data.productid_id
            ,forecast_data.forecast_quantity
            ,forecast_data.store_id
            ,production_product.product_nm
            ,sum(sales_salesorderdetail.orderqty_qt) as total_quantity_sold
            ,sum(sales_salesorderdetail.unitprice_vr * sales_salesorderdetail.orderqty_qt) as total_sales_value
            ,sum(sales_salesorderheader.subtotal_vr + sales_salesorderheader.taxamt_vr + sales_salesorderheader.freight_vr) as total_order_value
        from sales_salesorderheader
        join sales_salesorderdetail
             on sales_salesorderheader.salesorderid_id = sales_salesorderdetail.salesorderid_id
        left join {{ ref('stg_humanresources_employee') }}
             on sales_salesorderheader.salespersonid_id = stg_humanresources_employee.businessentityid_id
        left join {{ ref('stg_person_person') }}
             on stg_humanresources_employee.businessentityid_id = stg_person_person.businessentityid_id
        left join sales_store
             on sales_salesorderheader.salespersonid_id = sales_store.businessentityid_id_store
        left join forecast_data
             on sales_salesorderdetail.productid_id = forecast_data.productid_id
        left join production_product
             on sales_salesorderdetail.productid_id = production_product.productid_id
        group by
             sales_salesorderheader.territoryid_id
            ,sales_store.store_nm
            ,sales_salesorderheader.salespersonid_id
            ,stg_person_person.firstname_nm
            ,stg_person_person.lastname_nm
            ,stg_humanresources_employee.jobtitle_nm
            ,forecast_data.productid_id
            ,forecast_data.forecast_quantity
            ,forecast_data.store_id
            ,production_product.product_nm
)

select *
from final_fact_forecast
