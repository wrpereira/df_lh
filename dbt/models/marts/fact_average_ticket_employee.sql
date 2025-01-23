{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    aggregated_sales as (
        select
            sales_salesorderheader.salesorderid_id
            , sales_salesperson.businessentityid_id as salesperson_id
            , sales_salesperson.territoryid_id
            , sum(sales_salesorderheader.totaldue_vr) as total_revenue_per_order
        from {{ ref('stg_sales_salesorderheader') }} as sales_salesorderheader
        join {{ ref('stg_sales_salesperson') }} as sales_salesperson
            on sales_salesorderheader.salespersonid_id = sales_salesperson.businessentityid_id
        group by
            sales_salesorderheader.salesorderid_id
            , sales_salesperson.businessentityid_id
            , sales_salesperson.territoryid_id
    )

    , fact_average_ticket_employee as (
        select
            salesperson_id
            , count(salesorderid_id) as total_orders
            , round(sum(total_revenue_per_order), 2) as total_revenue_salesperson
            , {{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(salesorderid_id)') }} as average_ticket_salesperson
        from aggregated_sales
        group by
            salesperson_id
    )

select
    salesperson_id
    , total_orders
    , total_revenue_salesperson
    , average_ticket_salesperson
from fact_average_ticket_employee
