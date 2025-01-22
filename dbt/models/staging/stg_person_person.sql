{{ config(materialized='table') }}

with 
    renamed as (
        select
             businessentityid as businessentityid_id
            ,persontype as persontype_tp
            ,firstname as firstname_nm
            ,lastname as lastname_nm
            ,fullname as fullname_nm
            ,emailpromotion as emailpromotion_desc
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'person_person') }}
    )

select *
from renamed
