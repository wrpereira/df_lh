{{ config(materialized='table') }}

with 
    renamed as (
        select
             cast(JSON_VALUE(data, '$.businessentityid') as int64) as businessentityid_id
            ,JSON_VALUE(data, '$.name') as name_nm
            ,cast(JSON_VALUE(data, '$.salespersonid') as int64) as salespersonid_id
            ,JSON_VALUE(data, '$.demographics') as demographics_desc
            ,JSON_VALUE(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data', 'sales_store') }}
    )

select *
from renamed

