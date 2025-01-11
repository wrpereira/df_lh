{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    aggregated_sales as (
        select
             header.salesorderid_id,
             salesperson.businessentityid_id as salesperson_id,
             salesperson.territoryid_id,
             sum(header.totaldue_vr) as total_revenue_per_order
        from {{ ref('stg_sales_salesorderheader') }} header
        join {{ ref('stg_sales_salesperson') }} salesperson
            on header.salespersonid_id = salesperson.businessentityid_id
        group by
             header.salesorderid_id,
             salesperson.businessentityid_id,
             salesperson.territoryid_id
    ),
    ticket_by_salesperson as (
        select
             salesperson_id,
             count(salesorderid_id) as total_orders,
             sum(total_revenue_per_order) as total_revenue_salesperson,
             {{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(salesorderid_id)') }} as average_ticket_salesperson
        from aggregated_sales
        group by
             salesperson_id
    )

select
     salesperson_id,
     total_orders,
     total_revenue_salesperson,
     average_ticket_salesperson
from ticket_by_salesperson
