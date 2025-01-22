{{ config(materialized='table') }}

{% macro calculate_average_ticket(revenue_col, orders_col) %}
    round({{ revenue_col }} / nullif({{ orders_col }}, 0), 2)
{% endmacro %}

with 
    aggregated_sales as (
        select
            sales_salesorderheader.salesorderid_id
            , sales_customer.customerid_id
            , sales_customer.personid_id
            , sales_customer.storeid_id
            , sales_customer.territoryid_id
            , sum(sales_salesorderheader.totaldue_vr) as total_revenue_per_order
        from {{ ref('stg_sales_salesorderheader') }} as sales_salesorderheader
        join {{ ref('stg_sales_customer') }} as sales_customer
            on sales_salesorderheader.customerid_id = sales_customer.customerid_id
        group by
            sales_salesorderheader.salesorderid_id
            , sales_customer.customerid_id
            , sales_customer.personid_id
            , sales_customer.storeid_id
            , sales_customer.territoryid_id
    )

    , ticket_by_customer as (
        select
            customerid_id
            , count(salesorderid_id) as total_orders
            , round(sum(total_revenue_per_order), 2) as total_revenue_customer
            , {{ calculate_average_ticket('sum(total_revenue_per_order)', 'count(salesorderid_id)') }} as average_ticket_customer
        from aggregated_sales
        group by
            customerid_id
    )

select
    customerid_id
    , total_orders
    , total_revenue_customer
    , average_ticket_customer
from ticket_by_customer
