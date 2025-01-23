{{ config(materialized='table') }}

with 
    renamed as (
        select
            territoryid as territoryid_id
            , name as territory_nm
            , countryregioncode as countryregioncode_cd
            , "group" as territory_group_tp
            , salesytd as salesytd_vr
            , saleslastyear as saleslastyear_vr
            , rowguid as rowguid_desc
            , modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'sales_salesterritory') }}
    )

select * 
from renamed
