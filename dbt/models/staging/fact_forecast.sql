{{ config(materialized='table') }}

with 
    product as (
        select
            productid
            ,name as product_name
            ,productnumber
        from {{ source('raw_data', 'production_product') }}
)
,store as (
    select
        businessentityid as store_id
        ,name as store_name
    from {{ source('raw_data', 'sales_store') }}
)
,forecast_data as (
    select
        productid
        ,store_id
        ,forecast_date
        ,forecast_quantity
    from {{ ref('mock_product_forecast') }}

)
,final_fact_forecast as (
    select
        forecast_data.productid
        ,product.name as product_name
        ,product.productnumber
        ,forecast_data.store_id
        ,store.name as store_name
        ,forecast_data.forecast_date
        ,sum(forecast_data.forecast_quantity) as total_forecast_quantity
    from forecast_data
    left join product
        on forecast_data.productid = product.productid
    left join store
        on forecast_data.store_id = store.businessentityid
    group by
        forecast_data.productid
        ,product.name
        ,product.productnumber
        ,forecast_data.store_id
        ,store.name
        ,forecast_data.forecast_date
)
select * 
from final_fact_forecast

