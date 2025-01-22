{{ config(materialized='table') }}

with 
    renamed as (
        select
            creditcardid as creditcardid_id
            , cardtype as cardtype_tp
            , cardnumber as cardnumber_nr
            , expmonth as expmonth_nr
            , expyear as expyear_nr
            , modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'sales_creditcard') }}
    )

select *
from renamed
