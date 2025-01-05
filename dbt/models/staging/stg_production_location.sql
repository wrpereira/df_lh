{{ config(materialized='table') }}

with 
    renamed as (
        select
             locationid as locationid_id
            ,name as location_nm
            ,costrate as costrate_vr
            ,availability as availability_nr
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'production_location') }}
    )

select *
from renamed
