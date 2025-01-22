{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    aggregated_sales as (
        select
            sales_salesorderheader.salesorderid_id
            , sales_store.businessentityid_id
            , sales_store.store_nm
            , sales_salesorderheader.territoryid_id
            , sum(sales_salesorderheader.totaldue_vr) as total_revenue_per_order
        from {{ ref('stg_sales_salesorderheader') }} as sales_salesorderheader
        join {{ ref('stg_sales_store') }} as sales_store
            on sales_salesorderheader.salespersonid_id = sales_store.salespersonid_id
        group by
            sales_salesorderheader.salesorderid_id
            , sales_store.businessentityid_id
            , sales_store.store_nm
            , sales_salesorderheader.territoryid_id
    )

    , ticket_by_store as (
        select
            businessentityid_id
            , store_nm
            , count(salesorderid_id) as total_orders
            , round(sum(total_revenue_per_order), 2) as total_revenue_store
            , {{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(salesorderid_id)') }} as average_ticket_store
        from aggregated_sales
        group by
            businessentityid_id
            , store_nm
    )

select
    businessentityid_id
    , store_nm
    , total_orders
    , total_revenue_store
    , average_ticket_store
from ticket_by_store
