{{ config(materialized="incremental") }}

with 
    renamed as (
        select
            cast(JSON_VALUE(data, '$.productid') as int64) as productid,
            JSON_VALUE(data, '$.name') as name,
            JSON_VALUE(data, '$.productnumber') as productnumber,
            JSON_VALUE(data, '$.makeflag') as makeflag,
            JSON_VALUE(data, '$.finishedgoodsflag') as finishedgoodsflag,
            cast(JSON_VALUE(data, '$.safetystocklevel') as int64) as safetystocklevel,
            cast(JSON_VALUE(data, '$.reorderpoint') as int64) as reorderpoint,
            cast(JSON_VALUE(data, '$.standardcost') as numeric) as standardcost,
            cast(JSON_VALUE(data, '$.listprice') as numeric) as listprice,
            JSON_VALUE(data, '$.size') as size,
            JSON_VALUE(data, '$.color') as color,
            cast(JSON_VALUE(data, '$.weight') as numeric) as weight,
            JSON_VALUE(data, '$.productcategoryid') as productcategoryid,
            JSON_VALUE(data, '$.productsubcategoryid') as productsubcategoryid,
            JSON_VALUE(data, '$.rowguid') as rowguid,
            parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.modifieddate')) as modifieddate
        from {{ source('raw_data', 'production_product') }}
    )

select *
from renamed
