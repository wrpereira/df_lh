{{ config(materialized='table') }}

with 
    renamed as (
        select
            productid as productid_id
            , startdate as startdate_dt
            , enddate as enddate_dt
            , round((standardcost), 2) as standardcost_vr
            , modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'production_productcosthistory') }}
    )

select *
from renamed
