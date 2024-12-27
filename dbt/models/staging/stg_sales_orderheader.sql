{{ config(materialized="incremental") }}

with 
    renamed as (
        select
            cast(JSON_VALUE(data, '$.salesorderid') as int64) as salesorderid,
            cast(JSON_VALUE(data, '$.revisionnumber') as int64) as revisionnumber,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.orderdate')) as orderdate,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.duedate')) as duedate,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.shipdate')) as shipdate,
            cast(JSON_VALUE(data, '$.status') as int64) as status,
            JSON_VALUE(data, '$.onlineorderflag') as onlineorderflag,
            JSON_VALUE(data, '$.salesordernumber') as salesordernumber,
            JSON_VALUE(data, '$.purchaseordernumber') as purchaseordernumber,
            JSON_VALUE(data, '$.accountnumber') as accountnumber,
            cast(JSON_VALUE(data, '$.customerid') as int64) as customerid,
            cast(JSON_VALUE(data, '$.salespersonid') as int64) as salespersonid,
            cast(JSON_VALUE(data, '$.territoryid') as int64) as territoryid,
            cast(JSON_VALUE(data, '$.billtoaddressid') as int64) as billtoaddressid,
            cast(JSON_VALUE(data, '$.shiptoaddressid') as int64) as shiptoaddressid,
            cast(JSON_VALUE(data, '$.shipmethodid') as int64) as shipmethodid,
            cast(JSON_VALUE(data, '$.creditcardid') as int64) as creditcardid,
            JSON_VALUE(data, '$.creditcardapprovalcode') as creditcardapprovalcode,
            cast(JSON_VALUE(data, '$.currencyrateid') as int64) as currencyrateid,
            cast(JSON_VALUE(data, '$.subtotal') as numeric) as subtotal,
            cast(JSON_VALUE(data, '$.taxamt') as numeric) as taxamt,
            cast(JSON_VALUE(data, '$.freight') as numeric) as freight,
            cast(JSON_VALUE(data, '$.totaldue') as numeric) as totaldue,
            JSON_VALUE(data, '$.comment') as comment,
            JSON_VALUE(data, '$.rowguid') as rowguid,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.modifieddate')) as modifieddate
        from {{ source('raw_data', 'sales_salesorderheader') }}
    )

select *
from renamed
