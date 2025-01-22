{{ config(materialized='table') }}

with 
    renamed as (
        select
            productsubcategoryid as productsubcategoryid_id
            , productcategoryid as productcategoryid_id
            , name as subcategory_nm
            , rowguid as rowguid_desc
            , modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'production_productsubcategory') }}
    )

select *
from renamed
