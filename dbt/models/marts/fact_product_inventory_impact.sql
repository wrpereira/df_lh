{{ config(materialized="table") }}

with 
    purchasing_details as (
        select
             stg_purchasing_purchaseorderdetail.productid_id
            ,sum(stg_purchasing_purchaseorderdetail.orderqty_nr) as total_order_qty
            ,sum(stg_purchasing_purchaseorderdetail.receivedqty_nr) as total_received_qty
            ,sum(stg_purchasing_purchaseorderdetail.rejectedqty_nr) as total_rejected_qty
        from {{ ref('stg_purchasing_purchaseorderdetail') }} stg_purchasing_purchaseorderdetail
        group by stg_purchasing_purchaseorderdetail.productid_id
    ),

    inventory_status as (
        select
             stg_production_productinventory.productid_id
            ,stg_production_productinventory.locationid_id
            ,stg_production_productinventory.quantity_qt as inventory_quantity
            ,stg_production_productinventory.modifieddate_dt
        from {{ ref('stg_production_productinventory') }} stg_production_productinventory
    ),

    product_inventory_impact as (
        select
             purchasing_details.productid_id
            ,sum(purchasing_details.total_order_qty) as total_ordered_quantity
            ,sum(purchasing_details.total_received_qty) as total_received_quantity
            ,sum(purchasing_details.total_rejected_qty) as total_rejected_quantity
            ,sum(inventory_status.inventory_quantity) as total_inventory_quantity
            ,case 
                 when sum(inventory_status.inventory_quantity) >= sum(purchasing_details.total_order_qty) then 'Sufficient'
                 when sum(inventory_status.inventory_quantity) < sum(purchasing_details.total_order_qty) then 'Insufficient'
                 else 'Unknown'
             end as inventory_coverage
        from purchasing_details
        left join inventory_status
             on purchasing_details.productid_id = inventory_status.productid_id
        group by purchasing_details.productid_id
    )

select *
from product_inventory_impact
