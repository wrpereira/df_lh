{{ config(materialized='table') }}

with 
    renamed as (
        select
             cast(JSON_VALUE(data, '$.productid') as int64) as productid_id
            ,JSON_VALUE(data, '$.name') as name_nm
            ,JSON_VALUE(data, '$.productnumber') as productnumber_cd
            ,JSON_VALUE(data, '$.makeflag') as makeflag_fl
            ,JSON_VALUE(data, '$.finishedgoodsflag') as finishedgoodsflag_fl
            ,JSON_VALUE(data, '$.color') as color_tp
            ,cast(JSON_VALUE(data, '$.safetystocklevel') as int64) as safetystocklevel_nr
            ,cast(JSON_VALUE(data, '$.reorderpoint') as int64) as reorderpoint_tp
            ,cast(JSON_VALUE(data, '$.standardcost') as numeric) as standardcost_vl
            ,cast(JSON_VALUE(data, '$.listprice') as numeric) as listprice_vl
            ,JSON_VALUE(data, '$.size') as size_tp
            ,JSON_VALUE(data, '$.sizeunitmeasurecode') as sizeunitmeasurecode_cd
            ,JSON_VALUE(data, '$.weightunitmeasurecode') as weightunitmeasurecode_cd
            ,cast(JSON_VALUE(data, '$.weight') as numeric) as weight_tp
            ,JSON_VALUE(data, '$.daystomanufacture') as daystomanufacture_nr
            ,JSON_VALUE(data, '$.productline') as productline_tp
            ,JSON_VALUE(data, '$.class') as class_tp
            ,JSON_VALUE(data, '$.style') as style_tp
            ,JSON_VALUE(data, '$.productsubcategoryid') as productsubcategoryid_id
            ,JSON_VALUE(data, '$.productmodelid') as productmodelid_id
            ,parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.sellstartdate')) as sellstartdate_ts
            ,parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.sellenddate')) as sellenddate_ts
            ,parse_timestamp('%Y-%m-%dT%H:%M:%S', JSON_VALUE(data, '$.discontinueddate')) as discontinueddate_ts
            ,JSON_VALUE(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', JSON_VALUE(data, '$.modifieddate')) as modifieddate_ts
        from {{ source('raw_data', 'production_product') }}
    )

select *
from renamed

