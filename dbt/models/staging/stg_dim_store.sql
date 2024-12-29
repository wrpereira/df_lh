{{ config(materialized="table") }}

with 
    stg_sales_salesterritory as (
        select
            territoryid_id
            ,territory_name_nm
            ,countryregioncode_desc
            ,territory_group_tp
            ,rowguid_desc
            ,modifieddate_ts
        from {{ ref('stg_sales_salesterritory') }}
    ),
    stg_person_address as (
        select
            addressid_id
            ,addressline1_desc
            ,addressline2_desc
            ,city_desc
            ,stateprovinceid_id
            ,postalcode_desc
            ,rowguid_desc
            ,modifieddate_ts
        from {{ ref('stg_person_address') }}
    )

select
    stg_sales_salesterritory.territoryid_id
    ,stg_sales_salesterritory.territory_name_nm
    ,stg_sales_salesterritory.countryregioncode_desc
    ,stg_sales_salesterritory.territory_group_tp
    ,stg_sales_salesterritory.rowguid_desc as territory_rowguid_desc
    ,stg_sales_salesterritory.modifieddate_ts as territory_modifieddate_ts
    ,stg_person_address.addressid_id
    ,stg_person_address.addressline1_desc
    ,stg_person_address.addressline2_desc
    ,stg_person_address.city_desc
    ,stg_person_address.stateprovinceid_id
    ,stg_person_address.postalcode_desc
    ,stg_person_address.rowguid_desc as address_rowguid_desc
    ,stg_person_address.modifieddate_ts as address_modifieddate_ts
from stg_sales_salesterritory
left join stg_person_address
    on stg_sales_salesterritory.territoryid_id = stg_person_address.stateprovinceid_id
