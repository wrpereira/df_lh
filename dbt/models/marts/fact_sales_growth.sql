{{ config(materialized='table') }}

with 
    sales_with_dates as (
        select 
            fact_sales.productid_id
            , fact_sales.orderdate_dt
            , dim_date.year_nr
            , dim_date.month_nr
            , round(sum(fact_sales.total_sales_value), 2) as total_sales 
        from {{ ref('fact_sales') }} as fact_sales
        join {{ ref('dim_date') }} as dim_date
            on cast(fact_sales.orderdate_dt as date) = dim_date.orderdate_dt
        group by
            fact_sales.productid_id
            , fact_sales.orderdate_dt
            , dim_date.year_nr
            , dim_date.month_nr
    )

    , daily_growth as (
        select 
            productid_id
            , orderdate_dt
            , year_nr
            , month_nr
            , total_sales
            , round(lag(total_sales) over (partition by productid_id order by orderdate_dt), 2) as previous_day_sales
            , round((total_sales - lag(total_sales) over (partition by productid_id order by orderdate_dt)) / lag(total_sales) over (partition by productid_id order by orderdate_dt), 2) as growth_rate
            , 'daily' as growth_type
        from sales_with_dates
    )

    , monthly_growth as (
        select 
            DATE(year_nr, month_nr, 1) as orderdate_dt
            , productid_id
            , year_nr
            , month_nr
            , round(sum(total_sales), 2) as total_sales
            , round(IFNULL(lag(sum(total_sales)) over (partition by productid_id order by year_nr, month_nr), 0), 2) as previous_month_sales
            , round((sum(total_sales) - IFNULL(lag(sum(total_sales)) over (partition by productid_id order by year_nr, month_nr), 0)) / IFNULL(lag(sum(total_sales)) over (partition by productid_id order by year_nr, month_nr), 1), 2) as growth_rate
            , 'monthly' as growth_type
        from sales_with_dates
        group by
            productid_id
            , year_nr
            , month_nr
    )

select 
    productid_id
    , orderdate_dt
    , year_nr
    , month_nr
    , total_sales
    , previous_day_sales
    , growth_rate
    , growth_type
from daily_growth

union all

select
    productid_id 
    , orderdate_dt
    , year_nr
    , month_nr
    , total_sales
    , previous_month_sales
    , growth_rate
    , growth_type
from monthly_growth
order by 
    orderdate_dt nulls last 
    , growth_type
