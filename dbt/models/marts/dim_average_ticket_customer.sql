{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    aggregated_sales as (
        select
             header.salesorderid_id,
             customer.customerid_id,
             customer.personid_id,
             customer.storeid_id,
             customer.territoryid_id,
             sum(header.totaldue_vr) as total_revenue_per_order
        from {{ ref('stg_sales_salesorderheader') }} header
        join {{ ref('stg_sales_customer') }} customer
            on header.customerid_id = customer.customerid_id
        group by
             header.salesorderid_id,
             customer.customerid_id,
             customer.personid_id,
             customer.storeid_id,
             customer.territoryid_id
    ),
    ticket_by_customer as (
        select
             customerid_id,
             count(salesorderid_id) as total_orders,
             sum(total_revenue_per_order) as total_revenue_customer,
             {{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(salesorderid_id)') }} as average_ticket_customer
        from aggregated_sales
        group by
             customerid_id
    )

select
     customerid_id,
     total_orders,
     total_revenue_customer,
     average_ticket_customer
from ticket_by_customer
