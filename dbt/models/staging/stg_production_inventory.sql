{{ config(materialized="incremental") }}

with 
    renamed as (
        select
            cast(JSON_VALUE(data, '$.productid') as int64) as productid,
            cast(JSON_VALUE(data, '$.locationid') as int64) as locationid,
            cast(JSON_VALUE(data, '$.shelf') as string) as shelf,
            cast(JSON_VALUE(data, '$.bin') as int64) as bin,
            cast(JSON_VALUE(data, '$.quantity') as int64) as quantity,
            JSON_VALUE(data, '$.rowguid') as rowguid,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.modifieddate')) as modifieddate
        from {{ source('raw_data', 'production_productinventory') }}
    )

select *
from renamed
