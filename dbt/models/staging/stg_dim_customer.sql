{{ config(materialized="table") }}

with 
    stg_sales_customer as (
        select
            customerid_id
            ,cast(personid_id as int64) as personid_id -- Garantir consistência
            ,storeid_id
            ,territoryid_id
            ,rowguid_desc
            ,modifieddate_ts
        from {{ ref('stg_sales_customer') }}
    ),
    stg_person_person as (
        select
            cast(businessentityid_id as int64) as businessentityid_id -- Garantir consistência
            ,firstname_nm
            ,middlename_nm
            ,lastname_nm
            ,rowguid_desc
            ,modifieddate_ts
        from {{ ref('stg_person_person') }}
    )

select 
    stg_sales_customer.customerid_id
    ,stg_sales_customer.personid_id
    ,stg_sales_customer.storeid_id
    ,stg_sales_customer.territoryid_id
    ,stg_sales_customer.rowguid_desc as customer_rowguid_desc
    ,stg_sales_customer.modifieddate_ts as customer_modifieddate_ts
    ,stg_person_person.firstname_nm
    ,stg_person_person.middlename_nm
    ,stg_person_person.lastname_nm
    ,stg_person_person.rowguid_desc as person_rowguid_desc
    ,stg_person_person.modifieddate_ts as person_modifieddate_ts
from stg_sales_customer
left join stg_person_person
    on cast(stg_sales_customer.personid_id as int64) = cast(stg_person_person.businessentityid_id as int64)
