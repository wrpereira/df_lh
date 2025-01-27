{{ config(materialized='table') }}

with 
    sales_salesorderheader as (
        select
            salesorderid_id as salesorderid_id_soh
            , salespersonid_id
            , customerid_id
            , creditcardid_id
            , territoryid_id
            , onlineorderflag_fl
            , cast(orderdate_dt as date) as orderdate_dt
            , cast(shipdate_dt as date) as shipdate_dt
            , cast(duedate_dt as date) as duedate_dt             
            , subtotal_vr
            , taxamt_vr
            , freight_vr
        from {{ ref('stg_sales_salesorderheader') }}
    )
        
    , sales_salesorderheadersalesreason as (
        select
            salesorderid_id
            , salesreasonid_id as salesreasonid_id_sr
        from {{ ref('stg_sales_salesorderheadersalesreason') }}
    )

    , sales_salesreason as (
        select
            salesreasonid_id
            , reason_desc
            , reasontype_tp
        from {{ ref('stg_sales_salesreason') }}
    )

    , dim_reason as (    
        select
            sales_salesreason.salesreasonid_id
            , sales_salesorderheader.salesorderid_id_soh
            , sales_salesreason.reason_desc
            , sales_salesreason.reasontype_tp
        from sales_salesorderheadersalesreason    
        left join sales_salesorderheader
            on  sales_salesorderheadersalesreason.salesorderid_id = sales_salesorderheader.salesorderid_id_soh
        left join sales_salesreason
            on sales_salesorderheadersalesreason.salesreasonid_id_sr = sales_salesreason.salesreasonid_id   
    )

select *
from dim_reason