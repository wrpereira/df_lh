{{ config(materialized="incremental") }}

with 
    renamed as (
        select
             cast(JSON_VALUE(data, '$.productid') as int64) as productid_id
            ,cast(JSON_VALUE(data, '$.locationid') as int64) as locationid_id
            ,cast(JSON_VALUE(data, '$.shelf') as string) as shelf_desc
            ,cast(JSON_VALUE(data, '$.bin') as int64) as bin_desc
            ,cast(JSON_VALUE(data, '$.quantity') as int64) as quantity_qt
            ,JSON_VALUE(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data', 'production_productinventory') }}
    )

select *
from renamed
{{ incremental_filter('modifieddate') }}
