{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    aggregated_sales as (
        select
             header.salesorderid_id,
             store.businessentityid_id as store_id,
             store.store_nm,
             header.territoryid_id,
             sum(header.totaldue_vr) as total_revenue_per_order
        from {{ ref('stg_sales_salesorderheader') }} header
        join {{ ref('stg_sales_store') }} store
            on header.salespersonid_id = store.salespersonid_id
        group by
             header.salesorderid_id,
             store.businessentityid_id,
             store.store_nm,
             header.territoryid_id
    ),
    ticket_by_store as (
        select
             store_id,
             store_nm,
             count(salesorderid_id) as total_orders,
             sum(total_revenue_per_order) as total_revenue_store,
             {{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(salesorderid_id)') }} as average_ticket_store
        from aggregated_sales
        group by
             store_id,
             store_nm
    )

select
     store_id,
     store_nm,
     total_orders,
     total_revenue_store,
     average_ticket_store
from ticket_by_store
