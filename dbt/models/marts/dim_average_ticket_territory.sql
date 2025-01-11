{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    aggregated_sales as (
        select
             header.salesorderid_id,
             header.territoryid_id,
             sum(header.totaldue_vr) as total_revenue_per_order
        from {{ ref('stg_sales_salesorderheader') }} as header
        where header.territoryid_id is not null -- Filtrando para evitar NULL em territory_id
        group by
             header.salesorderid_id,
             header.territoryid_id
    ),

    ticket_by_order as (
        select
             salesorderid_id as order_id,
             territoryid_id,
             total_revenue_per_order, -- Não faz média aqui, usa o dado calculado
             {{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(salesorderid_id)') }} as average_ticket_order
        from aggregated_sales
        group by
             salesorderid_id,
             territoryid_id,
             total_revenue_per_order
    ),

    ticket_by_territory as (
        select
             territoryid_id,
             count(order_id) as total_orders_territory,
             sum(total_revenue_per_order) as total_revenue_territory,
             {{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(order_id)') }} as average_ticket_territory
        from ticket_by_order
        group by
             territoryid_id
    )

select
     ticket_by_order.territoryid_id as territory_id,
     count(ticket_by_order.order_id) as total_orders,
     sum(ticket_by_order.total_revenue_per_order) as total_revenue,
     {{ calculate_average_ticket('sum(ticket_by_order.total_revenue_per_order)', 'count(ticket_by_order.order_id)') }} as average_ticket,
     sum(ticket_by_territory.total_orders_territory) as total_orders_territory,
     sum(ticket_by_territory.total_revenue_territory) as total_revenue_territory,
     {{ calculate_average_ticket('sum(ticket_by_territory.total_revenue_territory)', 'sum(ticket_by_territory.total_orders_territory)') }} as average_ticket_territory
from ticket_by_order
left join ticket_by_territory on ticket_by_order.territoryid_id = ticket_by_territory.territoryid_id
group by
     ticket_by_order.territoryid_id
