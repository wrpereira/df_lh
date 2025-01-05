{{ config(materialized='table') }}

with 
    renamed as (
        select
             businessentityid as businessentityid_id
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'person_businessentity') }}
    )

select *
from renamed
