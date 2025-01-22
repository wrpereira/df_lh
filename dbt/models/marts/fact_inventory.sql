{{ config(materialized='table') }}

with 
    production_product as (
        select
            productid_id
            , product_nm
            , productnumber_cd
            , standardcost_vr
            , listprice_vr
        from {{ ref('stg_production_product') }}
    )

    , product_inventory as (
        select
            productid_id
            , locationid_id
            , shelf_desc
            , bin_desc
            , quantity_qt
        from {{ ref('stg_production_productinventory') }}
    )

    , joined_data as (
        select
            production_product.productid_id
            , production_product.product_nm
            , production_product.productnumber_cd
            , production_product.standardcost_vr
            , production_product.listprice_vr
            , product_inventory.locationid_id
            , product_inventory.shelf_desc
            , product_inventory.bin_desc
            , product_inventory.quantity_qt
        from production_product
        join product_inventory
            on production_product.productid_id = product_inventory.productid_id
    )

    , final_fact_inventory as (
        select
            joined_data.productid_id
            , joined_data.product_nm
            , joined_data.productnumber_cd
            , joined_data.standardcost_vr
            , joined_data.listprice_vr
            , joined_data.locationid_id
            , joined_data.shelf_desc
            , joined_data.bin_desc
            , sum(joined_data.quantity_qt) as total_quantity
        from joined_data
        group by
            joined_data.productid_id
            , joined_data.product_nm
            , joined_data.productnumber_cd
            , joined_data.standardcost_vr
            , joined_data.listprice_vr
            , joined_data.locationid_id
            , joined_data.shelf_desc
            , joined_data.bin_desc
    )

select *
from final_fact_inventory
