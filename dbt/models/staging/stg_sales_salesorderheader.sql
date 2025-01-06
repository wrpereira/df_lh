{{ config(materialized='table') }}

with 
    renamed as (
        select
             salesorderid as salesorderid_id
            ,revisionnumber as revisionnumber_cd
            ,cast(orderdate as timestamp) as orderdate_dt
            ,duedate as duedate_dt
            ,shipdate as shipdate_dt
            ,status as status_st
            ,onlineorderflag as onlineorderflag_fl
            ,accountnumber as accountnumber_cd
            ,customerid as customerid_id
            ,territoryid as territoryid_id
            ,billtoaddressid as billtoaddressid_id
            ,shiptoaddressid as shiptoaddressid_id
            ,shipmethodid as shipmethodid_id
            ,creditcardid as creditcardid_id
            ,creditcardapprovalcode as creditcardapprovalcode_cd
            ,subtotal as subtotal_vr
            ,taxamt as taxamt_vr
            ,freight as freight_vr
            ,totaldue as totaldue_vr
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'sales_salesorderheader') }}
    )

select *
from renamed
