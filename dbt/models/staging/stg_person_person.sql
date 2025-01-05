{{ config(materialized='table') }}

with 
    renamed as (
        select
             businessentityid as businessentityid_id
            ,persontype as persontype_tp
            ,namestyle as namestyle_fl
            ,title as title_desc
            ,firstname as firstname_nm
            ,middlename as middlename_nm
            ,lastname as lastname_nm
            ,suffix as suffix_desc
            ,emailpromotion as emailpromotion_desc
            ,additionalcontactinfo as additionalcontactinfo_desc
            ,demographics as demographics_desc
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data', 'person-person') }}
    )

select *
from renamed
