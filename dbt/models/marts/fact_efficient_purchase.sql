{{ config(materialized="table") }}

with 
    purchasing_purchaseorderdetail as (
        select
            purchaseorderid_id
            , productid_id
            , orderqty_nr
            , unitprice_vr
            , duedate_dt
        from {{ ref('stg_purchasing_purchaseorderdetail') }}
    )

    , purchasing_purchaseorderheader as (
        select
            purchaseorderid_id
            , vendorid_id
            , orderdate_dt
            , shipdate_dt
        from {{ ref('stg_purchasing_purchaseorderheader') }}
    )

    , purchasing_vendor as (
        select
            businessentityid_id as vendor_id
            , vendor_name_nm
        from {{ ref('stg_purchasing_vendor') }}
    )

    , production_product as (
        select
            productid_id
            , product_nm
            , productsubcategoryid_id
        from {{ ref('stg_production_product') }}
    )

    , production_productsubcategory as (
        select
            productsubcategoryid_id
            , subcategory_nm
            , productcategoryid_id
        from {{ ref('stg_production_productsubcategory') }}
    )

    , production_productcategory as (
        select
            productcategoryid_id
            , category_nm
        from {{ ref('stg_production_productcategory') }}
    )

    , purchase_details as (
        select
            purchasing_purchaseorderdetail.productid_id
            , purchasing_purchaseorderdetail.orderqty_nr
            , purchasing_purchaseorderdetail.unitprice_vr
            , purchasing_purchaseorderdetail.purchaseorderid_id
            , purchasing_purchaseorderdetail.duedate_dt
            , (purchasing_purchaseorderdetail.orderqty_nr * purchasing_purchaseorderdetail.unitprice_vr) as total_cost
        from purchasing_purchaseorderdetail
    )

    , vendor_info as (
        select
            purchase_details.productid_id
            , purchase_details.total_cost
            , purchase_details.orderqty_nr    
            , purchase_details.duedate_dt        
            , purchasing_purchaseorderheader.vendorid_id
            , purchasing_purchaseorderheader.orderdate_dt
            , purchasing_purchaseorderheader.shipdate_dt
            , purchasing_vendor.vendor_name_nm
            , purchase_details.purchaseorderid_id
        from purchase_details
        join purchasing_purchaseorderheader
            on purchase_details.purchaseorderid_id = purchasing_purchaseorderheader.purchaseorderid_id
        join purchasing_vendor
            on purchasing_purchaseorderheader.vendorid_id = purchasing_vendor.vendor_id
    )

    , product_info as (
        select
            production_product.productid_id
            , production_product.product_nm
            , production_product.productsubcategoryid_id
        from production_product
    )

    , subcategory_info as (
        select
            production_productsubcategory.productsubcategoryid_id
            , production_productsubcategory.subcategory_nm
            , production_productsubcategory.productcategoryid_id
        from production_productsubcategory
    )

    , category_info as (
        select
            production_productcategory.productcategoryid_id
            , production_productcategory.category_nm
        from production_productcategory
    )

    , efficiency_analysis as (
        select
            product_info.productid_id
            , product_info.product_nm
            , category_info.category_nm
            , subcategory_info.subcategory_nm
            , vendor_info.vendorid_id
            , vendor_info.vendor_name_nm
            , vendor_info.purchaseorderid_id
            , vendor_info.orderdate_dt
            , vendor_info.shipdate_dt
            , vendor_info.duedate_dt
            , date_diff(shipdate_dt, orderdate_dt, day) as processing_time_days
            , case 
                when shipdate_dt > duedate_dt then date_diff(shipdate_dt, duedate_dt, day)
                else 0
            end as delivery_delay_days
            , sum(vendor_info.orderqty_nr) as total_quantity
            , round(sum(vendor_info.total_cost), 2) as total_cost
        from vendor_info
        join product_info
            on vendor_info.productid_id = product_info.productid_id
        join subcategory_info
            on product_info.productsubcategoryid_id = subcategory_info.productsubcategoryid_id
        join category_info
            on subcategory_info.productcategoryid_id = category_info.productcategoryid_id
        group by
            product_info.productid_id
            , product_info.product_nm
            , category_info.category_nm
            , subcategory_info.subcategory_nm
            , vendor_info.vendorid_id
            , vendor_info.vendor_name_nm
            , vendor_info.purchaseorderid_id
            , vendor_info.orderdate_dt
            , vendor_info.shipdate_dt
            , vendor_info.duedate_dt
    )

select *
from efficiency_analysis
