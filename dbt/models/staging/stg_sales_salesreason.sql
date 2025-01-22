{{ config(materialized='table') }}

with 
    renamed as (
        select
            salesreasonid as salesreasonid_id
            , name as reason_desc
            , reasontype as reasontype_tp
            , modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'sales_salesreason') }}
    )

select *
from renamed
