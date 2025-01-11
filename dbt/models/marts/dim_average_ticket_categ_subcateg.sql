{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    product_sales as (
        select
             sod.productid_id,
             sum(sod.unitprice_vr * sod.orderqty_qt) as total_revenue_per_product,
             count(distinct sod.salesorderid_id) as total_orders_per_product -- Corrigido para pedidos únicos
        from {{ ref('stg_sales_salesorderdetail') }} sod
        group by sod.productid_id
    ),
    product_categories as (
        select
             prod.productid_id,
             subcategory.subcategory_nm,
             category.category_nm
        from {{ ref('stg_production_product') }} prod
        join {{ ref('stg_production_productsubcategory') }} subcategory
             on prod.productsubcategoryid_id = subcategory.productsubcategoryid_id
        join {{ ref('stg_production_productcategory') }} category
             on subcategory.productcategoryid_id = category.productcategoryid_id
    ),
    category_and_subcategory_tickets as (
        select
             product_categories.category_nm as category_name,
             product_categories.subcategory_nm as subcategory_name,
             sum(product_sales.total_revenue_per_product) as total_revenue,
             sum(product_sales.total_orders_per_product) as total_orders, -- Corrigido para somar os pedidos
             {{ calculate_average_ticket('sum(product_sales.total_revenue_per_product)', 'sum(product_sales.total_orders_per_product)') }} as average_ticket
        from product_sales
        join product_categories
             on product_sales.productid_id = product_categories.productid_id
        group by
             product_categories.category_nm,
             product_categories.subcategory_nm
    )

select
     category_name,
     subcategory_name,
     total_orders,
     total_revenue,
     average_ticket
from category_and_subcategory_tickets
order by
     category_name,
     subcategory_name
