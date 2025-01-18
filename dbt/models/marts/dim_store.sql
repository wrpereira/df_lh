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

    person_stateprovince as (
        select
             stateprovinceid_id
            ,territoryid_id
            ,state_province_nm
        from {{ ref('stg_person_stateprovince') }}
    ),

    sales_store as (
        select
             businessentityid_id as store_id
            ,store_nm
            ,salespersonid_id
        from {{ ref('stg_sales_store') }}
    ),

    sales_customer as (
        select
             customerid_id
            ,storeid_id
            ,territoryid_id
        from {{ ref('stg_sales_customer') }}
    ),

    store_with_details as (
        select
             sales_store.store_id
            ,sales_store.store_nm
            ,sales_customer.territoryid_id
            ,COALESCE(sales_salesterritory.territory_nm, 'NO DATA') as territory
            ,COALESCE(sales_salesterritory.countryregioncode_cd, 'NO DATA') as country_region_code
            ,COALESCE(sales_salesterritory.territory_group_tp, 'NO DATA') as territory_group
        from sales_store
        left join sales_customer
             on sales_store.store_id = sales_customer.storeid_id
        left join sales_salesterritory
             on sales_customer.territoryid_id = sales_salesterritory.territoryid_id
    )

select *
from store_with_details