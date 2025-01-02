{{ config(materialized="table") }}

with 
    renamed as (
        select
            cast(json_value(data, '$.addressid') as int64) as addressid_id
            ,json_value(data, '$.addressline1') as addressline1_desc
            ,json_value(data, '$.addressline2') as addressline2_desc
            ,json_value(data, '$.city') as city_desc
            ,cast(json_value(data, '$.stateprovinceid') as int64) as stateprovinceid_id
            ,json_value(data, '$.postalcode') as postalcode_desc
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data_cleaned', 'person_address') }}
    )

select *
from renamed
