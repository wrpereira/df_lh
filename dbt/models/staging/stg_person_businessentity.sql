{{ config(materialized="incremental") }}

with 
    renamed as (
        select
            cast(JSON_VALUE(data, '$.businessentityid') as int64) as businessentityid,
            JSON_VALUE(data, '$.rowguid') as rowguid,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.modifieddate')) as modifieddate
        from {{ source('raw_data', 'person_businessentity') }}
    )

select *
from renamed
