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

    sales_salesperson as (
        select
             businessentityid_id
        from {{ ref('stg_sales_salesperson') }}
    ),

    sales_store as (
        select
             salespersonid_id
            ,store_nm
        from {{ ref('stg_sales_store') }}
    ),

    salesperson_with_store as (
        select
             sales_store.store_nm
            ,sales_store.salespersonid_id
            ,sales_salesperson.businessentityid_id
        from sales_store
        left join sales_salesperson
             on sales_store.salespersonid_id = sales_salesperson.businessentityid_id
    ),

    final_dim_store as (
        select
             person_address.addressid_id
            ,salesperson_with_store.store_nm             
            ,person_address.city_nm
            ,state_province_nm
            ,person_address.postalcode_cd
            ,sales_salesterritory.territoryid_id
            ,sales_salesterritory.territory_nm
            ,sales_salesterritory.countryregioncode_cd
            ,sales_salesterritory.territory_group_tp
        from sales_salesterritory
        left join person_stateprovince
             on sales_salesterritory.territoryid_id = person_stateprovince.territoryid_id
        left join person_address
             on person_stateprovince.stateprovinceid_id = person_address.stateprovinceid_id
        left join salesperson_with_store
             on salesperson_with_store.salespersonid_id = person_address.addressid_id
    )

select *
from final_dim_store
