{{ config(materialized='table') }}

with 
    renamed as (
        select
            businessentityid as businessentityid_id
            ,nationalidnumber as nationalidnumber_id
            ,loginid as loginid_id
            ,jobtitle as jobtitle_nm
            ,birthdate as birthdate_ts
            ,maritalstatus as marital_st
            ,gender as gender_tp
            ,hiredate as hiredate_ts
            ,salariedflag as salariedflag_fl
            ,vacationhours as vacationhours_nr
            ,sickleavehours as sickleavehours_nr
            ,currentflag as currentflag_fl
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_ts
            ,organizationnode as organizationnode_desc
        from {{ source('raw_data_cleaned', 'stg_humanresources_employee') }}
)

select *
from renamed

