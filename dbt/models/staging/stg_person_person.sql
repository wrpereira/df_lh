{{ config(materialized="table") }}

with 
    renamed as (
        select
            cast(json_value(data, '$.businessentityid') as int64) as businessentityid_id -- Garante que é INT64
            ,json_value(data, '$.firstname') as firstname_nm
            ,json_value(data, '$.middlename') as middlename_nm
            ,json_value(data, '$.lastname') as lastname_nm
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data_cleaned', 'person_person') }}
    )

select *
from renamed
