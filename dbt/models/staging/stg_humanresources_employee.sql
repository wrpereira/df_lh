{{ config(materialized='table') }}

with 
    renamed as (
        select
            cast(JSON_VALUE(data, '$.businessentityid') as int64) as businessentityid_id,
            cast(JSON_VALUE(data, '$.nationalidnumber') as string) as nationalidnumber_id,
            JSON_VALUE(data, '$.loginid') as loginid_id,
            cast(JSON_VALUE(data, '$.jobtitle') as string) as jobtitle_nm,
            parse_timestamp('%Y-%m-%d', JSON_VALUE(data, '$.birthdate')) as birthdate_ts,
            JSON_VALUE(data, '$.maritalstatus') as marital_st,
            JSON_VALUE(data, '$.gender') as gender_tp,
            parse_timestamp('%Y-%m-%d', JSON_VALUE(data, '$.hiredate')) as hiredate_ts,
            JSON_VALUE(data, '$.salariedflag') as salariedflag_fl,
            cast(JSON_VALUE(data, '$.vacationhours') as int64) as vacationhours_nr,
            cast(JSON_VALUE(data, '$.sickleavehours') as int64) as sickleavehours_nr,
            JSON_VALUE(data, '$.currentflag') as currentflag_fl,
            JSON_VALUE(data, '$.rowguid') as rowguid_desc,
            parse_timestamp('%Y-%m-%dT%H:%M:%E6S', JSON_VALUE(data, '$.modifieddate')) as modifieddate_ts,
            cast(JSON_VALUE(data, '$.organizationnode') as string) as organizationnode_desc
        from {{ source('raw_data', 'humanresources_employee') }}
    )

select *
from renamed

