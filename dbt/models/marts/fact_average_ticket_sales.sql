{{ config(materialized='table') }}

with 
    sales_salesorderheader as (
        select
            salesorderid_id
            , customerid_id
            , creditcardid_id
            , territoryid_id
            , cast(orderdate_dt as date) as orderdate_dt
            , shipdate_dt            
            , subtotal_vr
            , taxamt_vr
            , freight_vr
        from {{ ref('stg_sales_salesorderheader') }}
    )

    , sales_salesorderdetail as (
        select
            salesorderid_id
            , productid_id
            , orderqty_qt
            , unitprice_vr
            , unitpricediscount_vr
        from {{ ref('stg_sales_salesorderdetail') }}
    )

    , final_fact_sales as (
        select
            sales_salesorderheader.salesorderid_id
            , sales_salesorderheader.customerid_id
            , sales_salesorderheader.orderdate_dt
            , sales_salesorderheader.shipdate_dt
            , sales_salesorderheader.territoryid_id
            , sales_salesorderheader.creditcardid_id
            , sum(sales_salesorderdetail.orderqty_qt) as total_quantity
            , sum(sales_salesorderdetail.unitprice_vr * sales_salesorderdetail.orderqty_qt) as total_sales_value
            , round(sum(sales_salesorderheader.subtotal_vr + sales_salesorderheader.taxamt_vr + sales_salesorderheader.freight_vr), 2) as total_order_value
        from sales_salesorderheader
        join sales_salesorderdetail
            on sales_salesorderheader.salesorderid_id = sales_salesorderdetail.salesorderid_id
        group by
            sales_salesorderheader.salesorderid_id
            , sales_salesorderheader.customerid_id
            , sales_salesorderheader.orderdate_dt
            , sales_salesorderheader.shipdate_dt
            , sales_salesorderheader.territoryid_id
            , sales_salesorderheader.creditcardid_id
    )

    , ticket_medio as (
        select
            sum(total_sales_value) / count(distinct salesorderid_id) as average_ticket
        from final_fact_sales
    )

select *
from ticket_medio
