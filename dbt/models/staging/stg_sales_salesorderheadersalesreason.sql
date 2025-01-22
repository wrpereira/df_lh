{{ config(materialized='table') }}

with 
    renamed as (
        select
             salesorderid as salesorderid_id
            ,salesreasonid as salesreasonid_id
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'sales_salesorderheadersalesreason') }}
    )

select *
from renamed
