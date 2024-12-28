{{ config(materialized='table') }}

with 
    renamed as (
        select
             cast(JSON_VALUE(data, '$.businessentityid') as int64) as businessentityid_id
            ,JSON_VALUE(data, '$.firstname') as firstname_nm
            ,JSON_VALUE(data, '$.middlename') as middlename_nm
            ,JSON_VALUE(data, '$.lastname') as lastname_nm
            ,JSON_VALUE(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', JSON_VALUE(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data', 'person_person') }}
    )

select *
from renamed
