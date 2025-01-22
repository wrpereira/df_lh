{{ config(materialized='table') }}

with 
    renamed as (
        select
            purchaseorderid as purchaseorderid_id
            , purchaseorderdetailid as purchaseorderdetailid_id
            , duedate as duedate_dt
            , orderqty as orderqty_nr
            , productid as productid_id
            , unitprice as unitprice_vr
            , receivedqty as receivedqty_nr
            , rejectedqty as rejectedqty_nr
            , modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'purchasing_purchaseorderdetail') }}
    )

select *
from renamed
