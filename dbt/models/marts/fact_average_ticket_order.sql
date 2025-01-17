{{ config(materialized='table') }}

with 
    aggregated_sales as (
        select
             salesorderid_id
            ,orderdate_dt
            ,sum(totaldue_vr) as total_revenue_per_order
        from {{ ref('stg_sales_salesorderheader') }}
        group by
             salesorderid_id
            ,orderdate_dt
    ),

    ticket_by_order as (
        select
             salesorderid_id as order_id
            ,orderdate_dt
            ,round(total_revenue_per_order, 2) as average_ticket_order
        from aggregated_sales
    )

select *
from ticket_by_order
