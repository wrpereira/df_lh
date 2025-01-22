{{ config(materialized='table') }}

with 
    renamed as (
        select
            businessentityid as businessentityid_id
            , name as store_nm
            , salespersonid as salespersonid_id
            , rowguid as rowguid_desc
            , modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'sales_store') }}
    )

select *
from renamed
