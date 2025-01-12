{{ config(materialized='table') }}

with renamed as (
    select
         businessentityid as businessentityid_id
        ,accountnumber as accountnumber_cd
        ,name as vendor_name_nm
        ,creditrating as creditrating_nr
        ,preferredvendorstatus as preferredvendorstatus_fl
        ,activeflag as activeflag_fl
        ,modifieddate as modifieddate_dt
    from {{ source('raw_data_cleaned', 'purchasing_vendor') }}
)

select *
from renamed
