{{ config(materialized="table") }}

with 
    person_person as (
        select
            businessentityid_id
            , firstname_nm
            , lastname_nm
            , fullname_nm
        from {{ ref('stg_person_person') }}
    )

    , humanresources_employee as (
        select
            businessentityid_id
            , jobtitle_tp
            , birthdate_dt
            , hiredate_dt
            , gender_tp
            , vacationhours_nr
            , sickleavehours_nr   
            , currentflag_fl   
        from {{ ref('stg_humanresources_employee') }}
    )

    , dim_employee as (
        select
            person_person.businessentityid_id
            , person_person.fullname_nm
            , humanresources_employee.gender_tp
            , humanresources_employee.jobtitle_tp
            , humanresources_employee.birthdate_dt
            , humanresources_employee.hiredate_dt
            , humanresources_employee.vacationhours_nr
            , humanresources_employee.sickleavehours_nr  
            , humanresources_employee.currentflag_fl          
        from humanresources_employee
        left join person_person
            on humanresources_employee.businessentityid_id = person_person.businessentityid_id
    )

select *
from dim_employee
