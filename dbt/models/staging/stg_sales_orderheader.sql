{{ config(materialized='table') }}

with 
    renamed as (
        select
             JSON_VALUE(data, '$.salesorderid')  as salesorderid_id
            ,cast(JSON_VALUE(data, '$.revisionnumber') as int64) as revisionnumber_cd
            ,parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.orderdate')) as orderdate_ts
            ,parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.duedate')) as duedate_ts
            ,parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.shipdate')) as shipdate_ts
            ,cast(JSON_VALUE(data, '$.status') as int64) as status_st
            ,JSON_VALUE(data, '$.onlineorderflag') as onlineorderflag_fl
            ,JSON_VALUE(data, '$.purchaseordernumber') as purchaseordernumber_cd
            ,JSON_VALUE(data, '$.accountnumber') as accountnumber_cd
            ,cast(JSON_VALUE(data, '$.customerid') as int64) as customerid_id
            ,cast(JSON_VALUE(data, '$.salespersonid') as int64) as salespersonid_id
            ,cast(JSON_VALUE(data, '$.territoryid') as int64) as territoryid_id
            ,cast(JSON_VALUE(data, '$.billtoaddressid') as int64) as billtoaddressid_id
            ,cast(JSON_VALUE(data, '$.shiptoaddressid') as int64) as shiptoaddressid_id
            ,cast(JSON_VALUE(data, '$.shipmethodid') as int64) as shipmethodid_id
            ,cast(JSON_VALUE(data, '$.creditcardid') as int64) as creditcardid_id
            ,JSON_VALUE(data, '$.creditcardapprovalcode') as creditcardapprovalcode_cd
            ,cast(JSON_VALUE(data, '$.currencyrateid') as int64) as currencyrateid_id
            ,cast(JSON_VALUE(data, '$.subtotal') as numeric) as subtotal_vr
            ,cast(JSON_VALUE(data, '$.taxamt') as numeric) as taxamt_vr
            ,cast(JSON_VALUE(data, '$.freight') as numeric) as freight_vr
            ,cast(JSON_VALUE(data, '$.totaldue') as numeric) as totaldue_vr
            ,JSON_VALUE(data, '$.comment') as comment_desc
            ,JSON_VALUE(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', JSON_VALUE(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data', 'sales_salesorderheader') }}
    )

select *
from renamed

