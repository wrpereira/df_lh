{{ config(materialized='table') }}

with 
    sales_salesorderheader as (
        select
            salesorderid_id as salesorderid_id_soh
            , salespersonid_id
            , customerid_id
            , creditcardid_id
            , territoryid_id
            , onlineorderflag_fl
            , cast(orderdate_dt as date) as orderdate_dt
            , cast(shipdate_dt as date) as shipdate_dt
            , cast(duedate_dt as date) as duedate_dt             
            , subtotal_vr
            , taxamt_vr
            , freight_vr
        from {{ ref('stg_sales_salesorderheader') }}
    )

    , sales_salesorderdetail as (
        select
            salesorderid_id
            , productid_id
            , orderqty_qt
            , unitprice_vr
            , unitpricediscount_vr
        from {{ ref('stg_sales_salesorderdetail') }}
    )

    , production_product as (
        select
            productid_id
            , product_nm
        from {{ ref('stg_production_product') }}
    )

    , sales_salesperson as (
        select
            businessentityid_id
            , territoryid_id
        from {{ ref('stg_sales_salesperson') }}
    )

    , sales_customer as (
        select
            customerid_id
            , storeid_id
            , territoryid_id
        from {{ ref('stg_sales_customer') }}
    )

    , sales_store as (
        select
            businessentityid_id
            , store_nm
        from {{ ref('stg_sales_store') }}
    )

    , humanresources_employee as (
        select
            businessentityid_id
        from {{ ref('stg_humanresources_employee') }}

    )

    , purchasing_purchaseorderheader as (
        select
            employeeid_id
            , vendorid_id
        from {{ ref('stg_purchasing_purchaseorderheader') }}
    )

    , sales_salesterritory as (
        select
            territoryid_id
            , territory_nm
            , countryregioncode_cd
        from {{ ref('stg_sales_salesterritory') }}
    )

    , fact_sales as (
        select
            sales_salesorderheader.salesorderid_id_soh
            , sales_salesorderheader.customerid_id
            , sales_salesorderheader.creditcardid_id   
            , purchasing_purchaseorderheader.vendorid_id            
            , sales_salesperson.businessentityid_id as businessentityid_id_sales_person
            , humanresources_employee.businessentityid_id as businessentityid_id_employee               
            , sales_salesorderdetail.productid_id
            , production_product.product_nm
            , sales_store.businessentityid_id as businessentityid_id_store
            , sales_store.store_nm
            , sales_salesorderheader.territoryid_id   
            , sales_salesterritory.territory_nm
            , sales_salesterritory.countryregioncode_cd                     
            , sales_salesorderdetail.orderqty_qt
            , sales_salesorderdetail.unitprice_vr    
            , sum(sales_salesorderdetail.orderqty_qt) as total_quantity
            , (sales_salesorderdetail.unitprice_vr * sales_salesorderdetail.orderqty_qt) as total_sales_value
            , round(sum(sales_salesorderheader.subtotal_vr + sales_salesorderheader.taxamt_vr + sales_salesorderheader.freight_vr), 2) as total_order_value
            , sales_salesorderheader.onlineorderflag_fl
            , sales_salesorderheader.orderdate_dt
            , sales_salesorderheader.shipdate_dt
            , sales_salesorderheader.duedate_dt            
            , date_diff(sales_salesorderheader.shipdate_dt, sales_salesorderheader.orderdate_dt, day) as delivery_time_days
        from sales_salesorderheader
        join sales_salesorderdetail
            on sales_salesorderheader.salesorderid_id_soh = sales_salesorderdetail.salesorderid_id
        left join production_product
            on sales_salesorderdetail.productid_id = production_product.productid_id
        left join sales_salesperson
            on sales_salesorderheader.salespersonid_id = sales_salesperson.businessentityid_id      
        left join sales_customer
            on sales_salesorderheader.customerid_id = sales_customer.customerid_id
        left join sales_store
            on sales_customer.storeid_id = sales_store.businessentityid_id   
        left join humanresources_employee
            on sales_salesperson.businessentityid_id = humanresources_employee.businessentityid_id
        left join purchasing_purchaseorderheader
            on humanresources_employee.businessentityid_id = purchasing_purchaseorderheader.employeeid_id        
        left join sales_salesterritory
            on sales_salesorderheader.territoryid_id = sales_salesterritory.territoryid_id                       
        group by
            sales_salesorderheader.salesorderid_id_soh
            , sales_salesorderheader.customerid_id
            , sales_salesorderheader.creditcardid_id   
            , purchasing_purchaseorderheader.vendorid_id            
            , businessentityid_id_sales_person
            , businessentityid_id_employee               
            , sales_salesorderdetail.productid_id
            , production_product.product_nm
            , businessentityid_id_store
            , sales_store.store_nm
            , sales_salesorderheader.territoryid_id   
            , sales_salesorderheader.onlineorderflag_fl
            , sales_salesorderdetail.orderqty_qt
            , sales_salesorderdetail.unitprice_vr    
            , sales_salesorderheader.orderdate_dt
            , sales_salesorderheader.shipdate_dt
            , sales_salesorderheader.duedate_dt
            , sales_salesorderdetail.orderqty_qt
            , sales_salesorderdetail.unitprice_vr
            , sales_salesterritory.territory_nm
            , sales_salesterritory.countryregioncode_cd                 
    )

select *
from fact_sales
