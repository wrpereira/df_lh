with 
    historical_sales as (
        select
              sales_salesorderheader.orderdate_dt
             ,sales_salesorderdetail.productid_id
             ,sum(sales_salesorderdetail.orderqty_qt) as total_quantity_sold
        from {{ ref('stg_sales_salesorderheader') }} as sales_salesorderheader
        join {{ ref('stg_sales_salesorderdetail') }} as sales_salesorderdetail
             on sales_salesorderheader.salesorderid_id = sales_salesorderdetail.salesorderid_id
        group by 
             sales_salesorderheader.orderdate_dt
             ,sales_salesorderdetail.productid_id
    ),

    seasonality_analysis as (
        select
              concat(cast(extract(year from historical_sales.orderdate_dt) as string), '-'
                    ,lpad(cast(extract(month from historical_sales.orderdate_dt) as string), 2, '0')) as year_month -- Para facilitar o gráfico
             ,extract(year from historical_sales.orderdate_dt) as year
             ,extract(month from historical_sales.orderdate_dt) as month
             ,historical_sales.productid_id
             ,sum(historical_sales.total_quantity_sold) as total_quantity_sold
        from historical_sales
        where historical_sales.productid_id = 712  -- Produto escolhido para análise
        group by 
              year_month
             ,extract(year from historical_sales.orderdate_dt)
             ,extract(month from historical_sales.orderdate_dt)
             ,historical_sales.productid_id
)

select *
from seasonality_analysis
order by year_month
