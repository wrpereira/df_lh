{{ config(materialized="table") }}

with 
    sales_customer as (
        select
            customerid_id
            , personid_id
            , territoryid_id
        from {{ ref('stg_sales_customer') }}
    )

    , person_person as (
        select
            businessentityid_id
            , firstname_nm            
            , lastname_nm
            , fullname_nm
            , emailpromotion_desc
        from {{ ref('stg_person_person') }}
    )

    , dim_customer as (
        select 
            sales_customer.customerid_id
            , person_person.fullname_nm
            , sales_customer.personid_id
            , sales_customer.territoryid_id
            , person_person.emailpromotion_desc
        from sales_customer
        left join person_person
            on sales_customer.personid_id = person_person.businessentityid_id
        where person_person.firstname_nm is not null  
            and person_person.lastname_nm is not null  
    )

select *
from dim_customer
