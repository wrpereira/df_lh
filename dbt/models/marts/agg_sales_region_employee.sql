{{ config(materialized='table') }}

with 
    sales_salesorderheader as (
        select
             salesorderid_id as salesorderid_id_soh
            ,customerid_id
            ,creditcardid_id
            ,territoryid_id
            ,cast(orderdate_dt as date) as orderdate_dt
            ,cast(shipdate_dt as date) as shipdate_dt
            ,cast(duedate_dt as date) as duedate_dt             
            ,subtotal_vr
            ,taxamt_vr
            ,freight_vr
        from {{ ref('stg_sales_salesorderheader') }}
    ),

    sales_salesorderdetail as (
        select
             salesorderid_id
            ,productid_id
            ,orderqty_qt
            ,unitprice_vr
            ,unitpricediscount_vr
        from {{ ref('stg_sales_salesorderdetail') }}
    ),

    sales_salesterritory as (
        select
             territoryid_id
            ,territory_nm
            ,countryregioncode_cd
        from {{ ref('stg_sales_salesterritory') }}
    ),

    sales_salesperson as (
        select
             businessentityid_id
            ,territoryid_id
        from {{ ref('stg_sales_salesperson') }}
    ),

    humanresources_employee as (
        select
             businessentityid_id
        from {{ ref('stg_humanresources_employee') }}
    ),

    person_person as (
        select
             businessentityid_id
            ,firstname_nm
            ,lastname_nm
            ,fullname_nm
        from {{ ref('stg_person_person') }}
    ),

    agg_sales_region_employee as (
        select
             humanresources_employee.businessentityid_id as businessentityid_id_employee
            ,person_person.fullname_nm as employee_full_name
            ,sales_salesorderheader.territoryid_id            
            ,sales_salesterritory.territory_nm
            ,sales_salesterritory.countryregioncode_cd
            ,sum(sales_salesorderdetail.orderqty_qt) as total_quantity
            ,sum(sales_salesorderdetail.unitprice_vr * sales_salesorderdetail.orderqty_qt) as total_sales_value
            ,round(sum(sales_salesorderheader.subtotal_vr + sales_salesorderheader.taxamt_vr + sales_salesorderheader.freight_vr), 2) as total_order_value
        from sales_salesorderheader
        join sales_salesorderdetail
             on sales_salesorderheader.salesorderid_id_soh = sales_salesorderdetail.salesorderid_id
        left join sales_salesterritory
             on sales_salesorderheader.territoryid_id = sales_salesterritory.territoryid_id
        left join sales_salesperson
             on sales_salesterritory.territoryid_id = sales_salesperson.territoryid_id
        left join humanresources_employee
             on sales_salesperson.businessentityid_id = humanresources_employee.businessentityid_id
        left join person_person
             on person_person.businessentityid_id = humanresources_employee.businessentityid_id
        group by
             businessentityid_id_employee
            ,employee_full_name
            ,sales_salesorderheader.territoryid_id            
            ,sales_salesterritory.territory_nm
            ,sales_salesterritory.countryregioncode_cd
    )

select *
from agg_sales_region_employee