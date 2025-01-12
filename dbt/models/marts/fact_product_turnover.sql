{{ config(materialized='table') }}

with product_inventory as (
    select
         production_productinventory.productid_id
        ,production_product.product_nm
        ,production_productsubcategory.subcategory_nm
        ,production_productcategory.category_nm
        ,production_productinventory.quantity_qt
        ,production_product.safetystocklevel_nr
        ,production_product.reorderpoint_tp
        ,production_productcosthistory.standardcost_vr
        ,production_product.listprice_vr 
        ,round((production_product.listprice_vr - production_productcosthistory.standardcost_vr), 2) as profit_margin
        ,case
            when production_productinventory.quantity_qt < production_product.safetystocklevel_nr then 'Baixa Rotatividade'
            when production_productinventory.quantity_qt > production_product.reorderpoint_tp then 'Alta Rotatividade'
            else 'Normal'
        end as stock_status
        ,current_date as snapshot_date
    from {{ ref('stg_production_productinventory') }} as production_productinventory
    join {{ ref('stg_production_product') }} as production_product
        on production_productinventory.productid_id = production_product.productid_id
    left join {{ ref('stg_production_productsubcategory') }} as production_productsubcategory
        on production_product.productsubcategoryid_id = production_productsubcategory.productsubcategoryid_id
    left join {{ ref('stg_production_productcategory') }} as production_productcategory
        on production_productsubcategory.productcategoryid_id = production_productcategory.productcategoryid_id
    left join {{ ref('stg_production_productcosthistory') }} as production_productcosthistory
        on production_product.productid_id = production_productcosthistory.productid_id
    )
    
select *
from product_inventory
