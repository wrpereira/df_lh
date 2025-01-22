{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    stg_sales_salesorderdetail as (
        select
            sales_salesorderdetail.productid_id
            , sum(sales_salesorderdetail.unitprice_vr * sales_salesorderdetail.orderqty_qt) as total_revenue_per_product
            , count(distinct sales_salesorderdetail.salesorderid_id) as total_orders_per_product
        from {{ ref('stg_sales_salesorderdetail') }} as sales_salesorderdetail
        group by
            sales_salesorderdetail.productid_id
    )

    , product_category_details as (
        select
            production_product.productid_id
            , production_productsubcategory.subcategory_nm as subcategory_name
            , production_productcategory.category_nm as category_name
        from {{ ref('stg_production_product') }} as production_product
        join {{ ref('stg_production_productsubcategory') }} as production_productsubcategory
            on production_product.productsubcategoryid_id = production_productsubcategory.productsubcategoryid_id
        join {{ ref('stg_production_productcategory') }} as production_productcategory
            on production_productsubcategory.productcategoryid_id = production_productcategory.productcategoryid_id
    )

    , category_and_subcategory_average_tickets as (
        select
            product_category_details.category_name
            , product_category_details.subcategory_name
            , stg_sales_salesorderdetail.productid_id
            , round(sum(stg_sales_salesorderdetail.total_revenue_per_product), 2) as total_revenue
            , sum(stg_sales_salesorderdetail.total_orders_per_product) as total_orders
            , {{ calculate_average_ticket('sum(stg_sales_salesorderdetail.total_revenue_per_product)', 'sum(stg_sales_salesorderdetail.total_orders_per_product)') }} as average_ticket
        from stg_sales_salesorderdetail
        join product_category_details
            on stg_sales_salesorderdetail.productid_id = product_category_details.productid_id
        group by
            product_category_details.category_name
            , product_category_details.subcategory_name
            , stg_sales_salesorderdetail.productid_id
    )

select
    productid_id
    , category_name
    , subcategory_name
    , total_orders
    , total_revenue
    , average_ticket
from category_and_subcategory_average_tickets
order by
    category_name
    , subcategory_name
    , productid_id
