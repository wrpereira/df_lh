{{ config(materialized='table') }}

with 
    renamed as (
        select
              JSON_VALUE(data, '$.salesorderid') as salesorderid_id
            , JSON_VALUE(data, '$.salesorderdetailid') as salesorderdetailid_id
            , JSON_VALUE(data, '$.carriertrackingnumber') as carriertrackingnumber_cd
            , cast(JSON_VALUE(data, '$.orderqty') as int64) as orderqty_qt
            , cast(JSON_VALUE(data, '$.productid') as int64) as productid_id
            , cast(JSON_VALUE(data, '$.specialofferid') as int64) as specialofferid_id
            , cast(JSON_VALUE(data, '$.unitprice') as numeric) as unitprice_vr
            , cast(JSON_VALUE(data, '$.unitpricediscount') as numeric) as unitpricediscount_vr
            , JSON_VALUE(data, '$.rowguid') as rowguid_desc
            , parse_timestamp('%Y-%m-%dT%H:%M:%E6S', JSON_VALUE(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data', 'sales_salesorderdetail') }}
)

select *
from renamed

