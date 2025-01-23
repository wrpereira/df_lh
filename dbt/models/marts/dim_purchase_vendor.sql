{{ config(materialized="table") }}

with 
    purchasing_vendor as (
        select
            businessentityid_id as vendor_id
            , vendor_name_nm
            , creditrating_nr
        from {{ ref('stg_purchasing_vendor') }}
    )

    , purchasing_purchaseorderheader as (
        select
            purchaseorderid_id
            , vendorid_id
            , subtotal_vr
            , taxamt_vr
            , freight_vr
            , orderdate_dt
        from {{ ref('stg_purchasing_purchaseorderheader') }}
    )

    , dim_purchase_vendor as (
        select
            purchasing_purchaseorderheader.vendorid_id as vendor_id
            , purchasing_vendor.vendor_name_nm
            , count(purchasing_purchaseorderheader.purchaseorderid_id) as total_orders
            , round(sum(purchasing_purchaseorderheader.subtotal_vr + purchasing_purchaseorderheader.taxamt_vr + purchasing_purchaseorderheader.freight_vr), 2) as total_spent
        from purchasing_purchaseorderheader
        join purchasing_vendor
            on purchasing_purchaseorderheader.vendorid_id = purchasing_vendor.vendor_id
        group by
            purchasing_purchaseorderheader.vendorid_id
            , purchasing_vendor.vendor_name_nm
    )

select *
from dim_purchase_vendor
