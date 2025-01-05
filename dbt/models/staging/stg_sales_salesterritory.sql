{{ config(materialized='table') }}

with 
    renamed as (
        select
             territoryid as territoryid_id
            ,name as territory_nm
            ,countryregioncode as countryregioncode_cd
            ,"group" as group_tp
            ,salesytd as salesytd_vr
            ,saleslastyear as saleslastyear_vr
            ,costytd as costytd_vr
            ,costlastyear as costlastyear_vr
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data', 'sales-salesterritory') }}
    )

select *
from renamed
