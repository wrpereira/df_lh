{{ config(materialized="incremental") }}

with 
    renamed as (
        select
            cast(JSON_VALUE(data, '$.salesorderid') as int64) as salesorderid
            , cast(JSON_VALUE(data, '$.salesorderdetailid') as int64) as salesorderdetailid
            , JSON_VALUE(data, '$.carriertrackingnumber') as carriertrackingnumber
            , cast(JSON_VALUE(data, '$.orderqty') as int64) as orderqty
            , cast(JSON_VALUE(data, '$.productid') as int64) as productid
            , cast(JSON_VALUE(data, '$.specialofferid') as int64) as specialofferid
            , cast(JSON_VALUE(data, '$.unitprice') as numeric) as unitprice
            , cast(JSON_VALUE(data, '$.unitpricediscount') as numeric) as unitpricediscount
            , cast(JSON_VALUE(data, '$.linetotal') as numeric) as linetotal
            , JSON_VALUE(data, '$.rowguid') as rowguid
            , parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.modifieddate')) as modifieddate
        from {{ source('raw_data', 'sales_salesorderdetail') }}
)

select *
from renamed
