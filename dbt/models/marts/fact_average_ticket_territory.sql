{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    aggregated_sales as (
        select
             sales_salesorderheader.salesorderid_id
            ,sales_salesorderheader.territoryid_id
            ,sum(sales_salesorderheader.totaldue_vr) as total_revenue_per_order
        from {{ ref('stg_sales_salesorderheader') }} as sales_salesorderheader
        where sales_salesorderheader.territoryid_id is not null
        group by
             sales_salesorderheader.salesorderid_id
            ,sales_salesorderheader.territoryid_id
    ),

    ticket_by_order as (
        select
             salesorderid_id as order_id
            ,territoryid_id
            ,total_revenue_per_order
            ,{{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(salesorderid_id)') }} as average_ticket_order
        from aggregated_sales
        group by
             salesorderid_id
            ,territoryid_id
            ,total_revenue_per_order
    ),

    ticket_by_territory as (
        select
             territoryid_id
            ,count(order_id) as total_orders_territory
            ,sum(total_revenue_per_order) as total_revenue_territory
            ,{{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(order_id)') }} as average_ticket_territory
        from ticket_by_order
        group by
             territoryid_id
    )

select
     ticket_by_order.territoryid_id as territory_id
    ,count(ticket_by_order.order_id) as total_orders
    ,round(sum(ticket_by_order.total_revenue_per_order), 2) as total_revenue
    ,{{ calculate_average_ticket('sum(ticket_by_order.total_revenue_per_order)', 'count(ticket_by_order.order_id)') }} as average_ticket
    ,round(sum(ticket_by_territory.total_orders_territory), 2) as total_orders_territory
    ,round(sum(ticket_by_territory.total_revenue_territory), 2) as total_revenue_territory
    ,{{ calculate_average_ticket('sum(ticket_by_territory.total_revenue_territory)', 'sum(ticket_by_territory.total_orders_territory)') }} as average_ticket_territory
from ticket_by_order
left join ticket_by_territory 
     on ticket_by_order.territoryid_id = ticket_by_territory.territoryid_id
group by
     ticket_by_order.territoryid_id
