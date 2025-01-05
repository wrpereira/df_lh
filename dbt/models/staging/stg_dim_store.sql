{{ config(materialized="table") }}

with 
    sales_salesterritory as (
        select
             territoryid_id
            ,territory_nm
            ,countryregioncode_cd
            ,group_tp as territory_group_tp
            ,modifieddate_dt
        from {{ ref('stg_sales_salesterritory') }}
    ),
    
    person_address as (
        select
             addressid_id
            ,city_nm
            ,stateprovinceid_id
            ,postalcode_cd
            ,modifieddate_dt
        from {{ ref('stg_person_address') }}
    ),

    person_stateprovince as (
        select
             stateprovinceid_id
            ,territoryid_id
            ,state_province_nm
        from {{ ref('stg_person_stateprovince') }}
    ),

    final_dim_store as (
        select
             person_address.addressid_id
            ,person_address.city_nm
            ,state_province_nm
            ,person_address.postalcode_cd
            ,sales_salesterritory.territoryid_id
            ,sales_salesterritory.territory_nm
            ,sales_salesterritory.countryregioncode_cd
            ,sales_salesterritory.territory_group_tp
            ,sales_salesterritory.modifieddate_dt as territory_modifieddate_dt
            ,person_address.modifieddate_dt as address_modifieddate_dt
        from sales_salesterritory
        left join person_stateprovince
             on sales_salesterritory.territoryid_id = person_stateprovince.territoryid_id
        left join person_address
             on person_stateprovince.stateprovinceid_id = person_address.stateprovinceid_id
)

select *
from final_dim_store
