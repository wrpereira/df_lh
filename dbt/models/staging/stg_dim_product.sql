{{ config(materialized="table") }}

with 
    product as (
        select
            cast(json_value(data, '$.productid') as int64) as productid_id
            ,json_value(data, '$.name') as product_name_nm
            ,json_value(data, '$.productnumber') as product_number_desc
            ,json_value(data, '$.color') as color_nm
            ,cast(json_value(data, '$.listprice') as float64) as list_price_amt
            ,cast(json_value(data, '$.size') as float64) as size_amt
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
        from {{ ref('stg_production_product') }}
    ),
    product_subcategory as (
        select
            cast(json_value(data, '$.productsubcategoryid') as int64) as productsubcategoryid_id
            ,json_value(data, '$.name') as subcategory_name_nm
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
        from {{ ref('stg_production_productsubcategory') }}
    ),
    product_category as (
        select
            cast(json_value(data, '$.productcategoryid') as int64) as productcategoryid_id
            ,json_value(data, '$.name') as category_name_nm
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
        from {{ ref('stg_production_productcategory') }}
    )

select
    product.productid_id
    ,product.product_name_nm
    ,product.product_number_desc
    ,product.color_nm
    ,product.list_price_amt
    ,product.size_amt
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
    on product.productid_id = product_subcategory.productsubcategoryid_id
left join product_category
    on product_subcategory.productsubcategoryid_id = product_category.productcategoryid_id

