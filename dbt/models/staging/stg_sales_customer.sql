{{ config(materialized="table") }}

with 
    renamed as (
        select
            cast(json_value(data, '$.customerid') as int64) as customerid_id
            ,cast(json_value(data, '$.personid') as int64) as personid_id -- Garante que é INT64
            ,json_value(data, '$.storeid') as storeid_id
            ,json_value(data, '$.territoryid') as territoryid_id
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data_cleaned', 'sales_customer') }}
    )

select *
from renamed
