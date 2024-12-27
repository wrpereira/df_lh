{{ config(materialized='table') }}

with 
    renamed as (
        select
             cast(JSON_VALUE(data, '$.locationid') as int64) as locationid_id
            ,JSON_VALUE(data, '$.name') as name_nm
            ,cast(JSON_VALUE(data, '$.costrate') as numeric) as costrate_vr
            ,cast(JSON_VALUE(data, '$.availability') as int64) as availability_nr
            ,parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data', 'production_location') }}
    )

select *
from renamed
