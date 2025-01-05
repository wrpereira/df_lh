{{ config(materialized='table') }}

with 
    renamed as (
        select
             productid as productid_id
            ,locationid as locationid_id
            ,shelf as shelf_desc
            ,bin as bin_desc
            ,quantity as quantity_qt
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'production_productinventory') }}
    )

select *
from renamed
