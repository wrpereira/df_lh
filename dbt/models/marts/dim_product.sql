{{ config(materialized="table") }}

with 
    production_product as (
        select
            productid_id
            , product_nm
            , color_tp
            , listprice_vr
            , size_tp
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

    , final_dim_product as (
        select
            production_product.productid_id
            , production_product.product_nm
            , coalesce(production_productcategory.category_nm, 'NO CATEGORY') as category_nm
            , coalesce(production_productsubcategory.subcategory_nm, 'NO SUBCATEGORY') as subcategory_nm                     
            , production_product.color_tp
            , production_product.size_tp
            , production_product.listprice_vr
        from production_product
        left join production_productsubcategory
            on production_product.productsubcategoryid_id = production_productsubcategory.productsubcategoryid_id
        left join production_productcategory
            on production_productsubcategory.productcategoryid_id = production_productcategory.productcategoryid_id
    )

select *
from final_dim_product
