{{ config(materialized='table') }}

with 
    renamed as (
        select
             stateprovinceid as stateprovinceid_id
            ,stateprovincecode as stateprovincecode_cd
            ,countryregioncode as countryregioncode_cd
            ,isonlystateprovinceflag as isonlystateprovinceflag_fl
            ,name as state_province_nm
            ,territoryid as territoryid_id
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'person_stateprovince') }}
    )

select *
from renamed
