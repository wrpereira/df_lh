{{ config(materialized='table') }}

with 
    renamed as (
        select
             purchaseorderid as purchaseorderid_id
            ,revisionnumber as revisionnumber_desc
            ,status as status_st
            ,employeeid as employeeid_id
            ,vendorid as vendorid_id
            ,shipmethodid as shipmethodid_id
            ,orderdate as orderdate_dt
            ,shipdate as shipdate_dt
            ,subtotal as subtotal_vr
            ,taxamt as taxamt_vr
            ,freight as freight_vr
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'purchasing_purchaseorderheader') }}
    )

select *
from renamed
