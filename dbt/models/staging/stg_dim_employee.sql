{{ config(materialized="table") }}

with 
    stg_humanresources_employee as (
        select
            businessentityid_id
            ,jobtitle_desc
            ,birthdate_ts
            ,hiredate_ts
            ,gender_desc
            ,payfrequency_nr
            ,rowguid_desc
            ,modifieddate_ts
        from {{ ref('stg_humanresources_employee') }}
    ),
    stg_person_person as (
        select
            businessentityid_id
            ,firstname_nm
            ,middlename_nm
            ,lastname_nm
            ,emailaddress_desc
            ,phonenumber_desc
            ,rowguid_desc
            ,modifieddate_ts
        from {{ ref('stg_person_person') }}
    )

select
    stg_humanresources_employee.businessentityid_id
    ,stg_humanresources_employee.jobtitle_desc
    ,stg_humanresources_employee.birthdate_ts
    ,stg_humanresources_employee.hiredate_ts
    ,stg_humanresources_employee.gender_desc
    ,stg_humanresources_employee.payfrequency_nr
    ,stg_humanresources_employee.rowguid_desc as employee_rowguid_desc
    ,stg_humanresources_employee.modifieddate_ts as employee_modifieddate_ts
    ,stg_person_person.firstname_nm
    ,stg_person_person.middlename_nm
    ,stg_person_person.lastname_nm
    ,stg_person_person.emailaddress_desc
    ,stg_person_person.phonenumber_desc
    ,stg_person_person.rowguid_desc as person_rowguid_desc
    ,stg_person_person.modifieddate_ts as person_modifieddate_ts
from stg_humanresources_employee
left join stg_person_person
    on stg_humanresources_employee.businessentityid_id = stg_person_person.businessentityid_id
