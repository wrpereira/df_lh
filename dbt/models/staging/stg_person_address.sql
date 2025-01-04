{{ config(materialized='table') }}

with 
    renamed as (
        select
            addressid as addressid_id
            ,addressline1 as addressline1_desc
            ,addressline2 as addressline2_desc
            ,city as city_nm
            ,stateprovinceid as stateprovinceid_id
            ,postalcode as postalcode_desc
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'stg_person_address') }}
    )

select *
from renamed
