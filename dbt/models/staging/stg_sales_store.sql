{{ config(materialized='table') }}

with 
    renamed as (
        select
             businessentityid as businessentityid_id
            ,name as store_nm
            ,salespersonid as salespersonid_id
            ,demographics as demographics_desc
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data', 'sales-store') }}
    )

select *
from renamed
