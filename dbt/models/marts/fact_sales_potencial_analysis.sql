{{ config(materialized='table') }}

with 

-- Base Sales Order Header
sales_order_header as (
    select
         salesorderid_id
        ,territoryid_id
        ,orderdate_dt
        ,totaldue_vr
    from {{ ref('stg_sales_salesorderheader') }}
),

-- Base Sales Territory
sales_territory as (
    select
         territoryid_id
        ,territory_nm
        ,salesytd_vr * 1000000 as salesytd_vr -- Ajuste de escala
        ,saleslastyear_vr * 1000000 as saleslastyear_vr -- Ajuste de escala
        ,countryregioncode_cd
    from {{ ref('stg_sales_salesterritory') }}
),

-- Joining Sales Orders with Territory
sales_with_territory as (
    select
         soh.territoryid_id
        ,st.territory_nm
        ,st.salesytd_vr
        ,st.saleslastyear_vr
        ,st.countryregioncode_cd
        ,sum(soh.totaldue_vr) as total_sales_value
        ,count(soh.salesorderid_id) as total_orders
        ,min(soh.orderdate_dt) as first_order_date
        ,max(soh.orderdate_dt) as last_order_date
    from sales_order_header soh
    left join sales_territory st
        on soh.territoryid_id = st.territoryid_id
    group by
         soh.territoryid_id
        ,st.territory_nm
        ,st.salesytd_vr
        ,st.saleslastyear_vr
        ,st.countryregioncode_cd
),

-- Enriching with Date Information (Optional)
final_sales_analysis as (
    select
         swt.territoryid_id
        ,swt.territory_nm
        ,swt.countryregioncode_cd
        ,round(swt.salesytd_vr, 2) as salesytd_vr -- Arredondamento
        ,round(swt.saleslastyear_vr, 2) as saleslastyear_vr -- Arredondamento
        ,swt.total_sales_value
        ,swt.total_orders
        ,swt.first_order_date
        ,swt.last_order_date
        ,extract(year from swt.first_order_date) as first_order_year
        ,extract(year from swt.last_order_date) as last_order_year
    from sales_with_territory swt
)

select * 
from final_sales_analysis
