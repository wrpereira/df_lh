{{ config(materialized="table") }}

with 
    product as (
        select
            productid_id
            ,name_nm
            ,productnumber_cd
            ,color_tp
            ,listprice_vr
            ,size_tp
            ,rowguid_desc
            ,modifieddate_ts
            ,cast(productsubcategoryid_id as int64) as productsubcategoryid -- Ajuste correto
        from {{ ref('stg_production_product') }}
    ),
    product_subcategory as (
        select
            cast(productsubcategoryid_id as int64) as productsubcategoryid_id
            ,subcategory_name_nm
            ,rowguid_desc
            ,modifieddate_ts
        from {{ ref('stg_production_productsubcategory') }}
    ),
    product_category as (
        select
            productcategoryid_id
            ,category_name_nm
            ,rowguid_desc
            ,modifieddate_ts
        from {{ ref('stg_production_productcategory') }}
    )

select
    product.productid_id
    ,product.name_nm
    ,product.productnumber_cd
    ,product.color_tp
    ,product.listprice_vr
    ,product.size_tp
    ,product.rowguid_desc as product_rowguid_desc
    ,product.modifieddate_ts as product_modifieddate_ts
    ,product_subcategory.subcategory_name_nm
    ,product_subcategory.rowguid_desc as subcategory_rowguid_desc
    ,product_subcategory.modifieddate_ts as subcategory_modifieddate_ts
    ,product_category.category_name_nm
    ,product_category.rowguid_desc as category_rowguid_desc
    ,product_category.modifieddate_ts as category_modifieddate_ts
from product
left join product_subcategory
    on product.productsubcategoryid = product_subcategory.productsubcategoryid_id
left join product_category
    on product_subcategory.productsubcategoryid_id = product_category.productcategoryid_id
