{{ config(materialized="incremental") }}

with 
    renamed as (
        select
            cast(JSON_VALUE(data, '$.businessentityid') as int64) as businessentityid,
            cast(JSON_VALUE(data, '$.nationalidnumber') as string) as nationalidnumber,
            JSON_VALUE(data, '$.loginid') as loginid,
            cast(JSON_VALUE(data, '$.jobtitle') as string) as jobtitle,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.birthdate')) as birthdate,
            JSON_VALUE(data, '$.maritalstatus') as maritalstatus,
            JSON_VALUE(data, '$.gender') as gender,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.hiredate')) as hiredate,
            JSON_VALUE(data, '$.salariedflag') as salariedflag,
            cast(JSON_VALUE(data, '$.vacationhours') as int64) as vacationhours,
            cast(JSON_VALUE(data, '$.sickleavehours') as int64) as sickleavehours,
            JSON_VALUE(data, '$.currentflag') as currentflag,
            JSON_VALUE(data, '$.rowguid') as rowguid,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.modifieddate')) as modifieddate
        from {{ source('raw_data', 'humanresources_employee') }}
    )

select *
from renamed
