{{ config(materialized="table") }}

with 
    renamed as (
        select
            cast(json_value(data, '$.territoryid') as int64) as territoryid_id
            ,json_value(data, '$.name') as territory_name_nm
            ,json_value(data, '$.countryregioncode') as countryregioncode_desc
            ,json_value(data, '$.group') as territory_group_tp
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data','sales_salesterritory') }}
    )

select *
from renamed
