{{ config(materialized='table') }}

with 
    unique_dates as (
        select distinct
            cast(orderdate_dt as date) as orderdate_dt
        from {{ ref('fact_sales') }}
        where orderdate_dt is not null 
    )

    , dim_date as (
        select
            orderdate_dt
            , extract(year from orderdate_dt) as year_nr
            , extract(month from orderdate_dt) as month_nr
            , extract(quarter from orderdate_dt) as quarter_nr
            , extract(week from orderdate_dt) as week_nr
            , extract(day from orderdate_dt) as day_nr
            , concat('Q', cast(extract(quarter from orderdate_dt) as string)) as quarter_name            
            , FORMAT_DATE('%B', orderdate_dt) AS month_name
            , FORMAT_DATE('%b', orderdate_dt) AS month_name_abr
            , date_trunc(orderdate_dt, month) as first_day_of_month
            , date_sub(date_trunc(date_add(orderdate_dt, interval 1 month), month), interval 1 day) as last_day_of_month
        from unique_dates
    )

select *
from dim_date
