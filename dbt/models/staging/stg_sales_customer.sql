{{ config(materialized='table') }}

with 
    renamed as (
        select
             customerid as customerid_id
            ,personid as personid_id
            ,storeid as storeid_id
            ,territoryid as territoryid_id
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'sales_customer') }}
    )

select *
from renamed
