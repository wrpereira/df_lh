{{ config(materialized='table') }}

with 
    renamed as (
        select
             businessentityid as businessentityid_id
            ,nationalidnumber as nationalidnumber_id
            ,loginid as loginid_id
            ,jobtitle as jobtitle_tp
            ,birthdate as birthdate_dt
            ,maritalstatus as marital_st
            ,gender as gender_tp
            ,hiredate as hiredate_dt
            ,salariedflag as salariedflag_fl
            ,vacationhours as vacationhours_nr
            ,sickleavehours as sickleavehours_nr
            ,currentflag as currentflag_fl
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
            ,organizationnode as organizationnode_desc
        from {{ source('raw_data_cleaned', 'humanresources_employee') }}
    )

select *
from renamed

