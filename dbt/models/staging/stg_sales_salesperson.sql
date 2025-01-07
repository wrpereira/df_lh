{{ config(materialized='table') }}

with 
    renamed as (
        select
             businessentityid as businessentityid_id
            ,territoryid as territoryid_id
            ,salesquota as salesquota_nr
            ,bonus as bonus_vr
            ,commissionpct as commissionpct_vr
            ,salesytd as salesytd_vr
            ,saleslastyear as saleslastyear_vr
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'sales_salesperson') }}
)

select *
from renamed
