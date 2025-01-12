{{ config(materialized='table') }}

with 
    date_bounds as (
        select
             cast(min(orderdate_dt) as date) as min_date
            ,cast(max(orderdate_dt) as date) as max_date
        from {{ ref('stg_sales_salesorderheader') }}
    ),

    dates as (
        select 
             date as full_date
        from unnest(generate_date_array(
             (select min_date from date_bounds)
            ,(select max_date from date_bounds)
        )) as date
    ),

    dim_date as (
        select
             full_date
            ,extract(year from full_date) as year_nr
            ,extract(month from full_date) as month_nr
            ,extract(day from full_date) as day_nr
            ,extract(quarter from full_date) as quarter_nr
            ,format_date('%B', full_date) as month_name
            ,concat('Q', cast(extract(quarter from full_date) as string)) as quarter_name
            ,extract(week from full_date) as week_nr
            ,case when extract(dayofweek from full_date) in (1, 7) then true else false end as is_weekend
            ,case 
                when full_date in ('2011-12-25', '2012-01-01', '2012-07-04', --feriados comuns
                                '2012-12-25', '2013-01-01', '2013-07-04',
                                '2013-12-25', '2014-01-01', '2014-07-04') 
                then true 
                else false 
             end as is_holiday
            ,date_trunc(full_date, month) as first_day_of_month
            ,date_sub(date_trunc(date_add(full_date, interval 1 month), month), interval 1 day) as last_day_of_month
        from dates
    )

select *
from dim_date
