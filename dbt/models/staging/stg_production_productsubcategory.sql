{{ config(materialized="table") }}

with 
    renamed as (
        select
            cast(json_value(data, '$.productsubcategoryid') as int64) as productsubcategoryid_id
            ,json_value(data, '$.name') as subcategory_name_nm
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data', 'production_productsubcategory') }}
    )

select *
from renamed
