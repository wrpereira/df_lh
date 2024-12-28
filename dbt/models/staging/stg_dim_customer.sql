{{ config(materialized="table") }}

with 
    stg_sales_customer as (
        select
            cast(json_value(data, '$.customerid') as int64) as customerid_id
            ,json_value(data, '$.personid') as personid_id
            ,json_value(data, '$.storeid') as storeid_id
            ,json_value(data, '$.territoryid') as territoryid_id
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
        from {{ red('stg_sales_customer') }}
    ),
    stg_person_person as (
        select
            cast(json_value(data, '$.businessentityid') as int64) as businessentityid_id
            ,json_value(data, '$.firstname') as firstname_nm
            ,json_value(data, '$.middlename') as middlename_nm
            ,json_value(data, '$.lastname') as lastname_nm
            ,json_value(data, '$.rowguid') as rowguid_desc
            ,parse_timestamp('%Y-%m-%dT%H:%M:%E6S', json_value(data, '$.modifieddate')) as modifieddate_ts
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
    on stg_sales_customer.personid_id = stg_person_person.businessentityid_id
