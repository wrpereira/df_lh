{{ config(materialized='table') }}

with 
    renamed as (
        select
             salesorderid as salesorderid_id
            ,salesorderdetailid as salesorderdetailid_id
            ,carriertrackingnumber as carriertrackingnumber_cd
            ,orderqty as orderqty_qt
            ,productid as productid_id
            ,specialofferid as specialofferid_id
            ,unitprice as unitprice_vr
            ,unitpricediscount as unitpricediscount_vr
            ,rowguid as rowguid_desc
            ,modifieddate as modifieddate_dt
        from {{ source('raw_data', 'sales-salesorderdetail') }}
    )

select *
from renamed
