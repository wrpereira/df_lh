{{ config(materialized='table') }}

with 
    renamed as (
        select
            productid as productid_id
            , name as product_nm
            , productnumber as productnumber_cd
            , makeflag as makeflag_fl
            , finishedgoodsflag as finishedgoodsflag_fl
            , color as color_tp
            , safetystocklevel as safetystocklevel_nr
            , reorderpoint as reorderpoint_tp
            , round((standardcost), 2) as standardcost_vr
            , listprice as listprice_vr
            , size as size_tp
            , sizeunitmeasurecode as sizeunitmeasurecode_cd
            , weightunitmeasurecode as weightunitmeasurecode_cd
            , weight as weight_tp
            , daystomanufacture as daystomanufacture_nr
            , productline as productline_tp
            , class as class_tp
            , style as style_tp
            , productsubcategoryid as productsubcategoryid_id
            , productmodelid as productmodelid_id
            , sellstartdate as sellstartdate_dt
            , sellenddate as sellenddate_dt
            , rowguid as rowguid_desc
            , modifieddate as modifieddate_dt
        from {{ source('raw_data_cleaned', 'production_product') }}
    )

select *
from renamed
