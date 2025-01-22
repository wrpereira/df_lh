{{ config(materialized='table') }}

with 
    renamed as (
        select
            productcategoryid as productcategoryid_id
            , name as category_nm
            , rowguid as rowguid_desc
            , modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'production_productcategory') }}
    )

select *
from renamed
