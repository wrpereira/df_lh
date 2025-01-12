{{ config(materialized="table") }}

with 
    purchasing_purchaseorderdetail as (
        select
             purchaseorderid_id
            ,productid_id
            ,orderqty_nr
            ,unitprice_vr
        from {{ ref('stg_purchasing_purchaseorderdetail') }}
    ),

    purchasing_purchaseorderheader as (
        select
             purchaseorderid_id
            ,vendorid_id
        from {{ ref('stg_purchasing_purchaseorderheader') }}
    ),

    purchasing_vendor as (
        select
             businessentityid_id as vendor_id
            ,vendor_name_nm
        from {{ ref('stg_purchasing_vendor') }}
    ),

    production_product as (
        select
             productid_id
            ,product_nm
            ,productsubcategoryid_id
        from {{ ref('stg_production_product') }}
    ),

    production_productsubcategory as (
        select
             productsubcategoryid_id
            ,subcategory_nm
            ,productcategoryid_id
        from {{ ref('stg_production_productsubcategory') }}
    ),

    production_productcategory as (
        select
             productcategoryid_id
            ,category_nm
        from {{ ref('stg_production_productcategory') }}
    ),

    purchase_details as (
        select
             purchasing_purchaseorderdetail.productid_id
            ,purchasing_purchaseorderdetail.orderqty_nr
            ,purchasing_purchaseorderdetail.unitprice_vr
            ,purchasing_purchaseorderdetail.purchaseorderid_id
            ,(purchasing_purchaseorderdetail.orderqty_nr * purchasing_purchaseorderdetail.unitprice_vr) as total_cost
        from purchasing_purchaseorderdetail
    ),

    vendor_info as (
        select
             purchase_details.productid_id
            ,purchase_details.total_cost
            ,purchase_details.orderqty_nr
            ,purchasing_purchaseorderheader.vendorid_id
            ,purchasing_vendor.vendor_name_nm
        from purchase_details
        join purchasing_purchaseorderheader
             on purchase_details.purchaseorderid_id = purchasing_purchaseorderheader.purchaseorderid_id
        join purchasing_vendor
             on purchasing_purchaseorderheader.vendorid_id = purchasing_vendor.vendor_id
    ),

    product_info as (
        select
             production_product.productid_id
            ,production_product.product_nm
            ,production_product.productsubcategoryid_id
        from production_product
    ),

    subcategory_info as (
        select
             production_productsubcategory.productsubcategoryid_id
            ,production_productsubcategory.subcategory_nm
            ,production_productsubcategory.productcategoryid_id
        from production_productsubcategory
    ),

    category_info as (
        select
             production_productcategory.productcategoryid_id
            ,production_productcategory.category_nm
        from production_productcategory
    ),

    efficiency_analysis as (
        select
             product_info.product_nm
            ,category_info.category_nm
            ,subcategory_info.subcategory_nm
            ,vendor_info.vendor_name_nm
            ,sum(vendor_info.orderqty_nr) as total_quantity
            ,round(sum(vendor_info.total_cost), 2) as total_cost
        from vendor_info
        join product_info
             on vendor_info.productid_id = product_info.productid_id
        join subcategory_info
             on product_info.productsubcategoryid_id = subcategory_info.productsubcategoryid_id
        join category_info
             on subcategory_info.productcategoryid_id = category_info.productcategoryid_id
        group by
             category_info.category_nm
            ,subcategory_info.subcategory_nm
            ,product_info.product_nm
            ,vendor_info.vendor_name_nm
    )

select *
from efficiency_analysis
