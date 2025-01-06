{{ config(materialized="table") }}

with 
    sales_customer as (
        select
             customerid_id
            ,personid_id
            ,storeid_id
            ,territoryid_id
            ,rowguid_desc
            ,modifieddate_dt
        from {{ ref('stg_sales_customer') }}
    ),

    person_person as (
        select
             businessentityid_id
            ,firstname_nm
            ,middlename_nm
            ,lastname_nm
            ,rowguid_desc
            ,modifieddate_dt
        from {{ ref('stg_person_person') }}
    ),

    final_dim_customer as (
        select 
             sales_customer.customerid_id
            ,sales_customer.personid_id
            ,sales_customer.storeid_id
            ,sales_customer.territoryid_id
            ,sales_customer.rowguid_desc as customer_rowguid_desc
            ,sales_customer.modifieddate_dt as customer_modifieddate_dt
            ,person_person.firstname_nm
            ,person_person.middlename_nm
            ,person_person.lastname_nm
            ,person_person.rowguid_desc as person_rowguid_desc
            ,person_person.modifieddate_dt as person_modifieddate_dt
        from sales_customer
        left join person_person
             on cast(sales_customer.personid_id as int64) = cast(person_person.businessentityid_id as int64)
)

select*
from final_dim_customer