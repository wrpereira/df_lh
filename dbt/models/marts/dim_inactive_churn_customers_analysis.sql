{{ config(materialized='table') }}

with 
    date_range as (
        select 
            cast(max(orderdate_dt) as date) as max_date -- Garante que max_date seja do tipo DATE
        from {{ ref('fact_sales') }}
    ),

    last_purchase as (
        select 
             fact_sales.customerid_id
            ,dim_customer.firstname_nm
            ,dim_customer.lastname_nm
            ,concat(dim_customer.firstname_nm, ' ', dim_customer.lastname_nm) as full_name -- Concatena os nomes
            ,cast(max(fact_sales.orderdate_dt) as date) as last_order_date -- Garante que last_order_date seja do tipo DATE
        from {{ ref('fact_sales') }} as fact_sales
        left join {{ ref('dim_customer') }} as dim_customer
            on fact_sales.customerid_id = dim_customer.customerid_id
        group by 
             fact_sales.customerid_id
            ,dim_customer.firstname_nm
            ,dim_customer.lastname_nm
    ),

    inactivity_analysis as (
        select
             last_purchase.customerid_id
            ,last_purchase.full_name -- Nome completo
            ,last_purchase.last_order_date
            ,date_diff(
                 (select max_date from date_range), -- Usa a data máxima no cálculo
                 last_purchase.last_order_date, 
                 DAY
             ) as days_since_last_purchase
            ,case 
                when date_diff(
                        (select max_date from date_range), 
                        last_purchase.last_order_date, 
                        DAY
                    ) > 180 then true
                else false
             end as is_inactive -- Critério de inatividade (180 dias)
            ,case 
                when date_diff(
                        (select max_date from date_range), 
                        last_purchase.last_order_date, 
                        DAY
                    ) > 365 then true -- Critério de churn (1 ano)
                else false
             end as is_churned
        from last_purchase
    )

select *
from inactivity_analysis
