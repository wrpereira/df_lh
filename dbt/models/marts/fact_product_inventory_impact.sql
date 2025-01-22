{{ config(materialized="table") }}

with 
    purchasing_purchaseorderdetail as (
        select
            purchasing_purchaseorderdetail.productid_id
            , sum(purchasing_purchaseorderdetail.orderqty_nr) as total_order_qty
            , sum(purchasing_purchaseorderdetail.receivedqty_nr) as total_received_qty
            , sum(purchasing_purchaseorderdetail.rejectedqty_nr) as total_rejected_qty
        from {{ ref('stg_purchasing_purchaseorderdetail') }} as purchasing_purchaseorderdetail
        group by 
            purchasing_purchaseorderdetail.productid_id
    )

    , inventory_status as (
        select
            production_productinventory.productid_id
            , production_productinventory.locationid_id
            , production_productinventory.quantity_qt as inventory_quantity
            , production_productinventory.modifieddate_dt
        from {{ ref('stg_production_productinventory') }} as production_productinventory
    )

    , product_inventory_impact as (
        select
            purchasing_purchaseorderdetail.productid_id
            , sum(purchasing_purchaseorderdetail.total_order_qty) as total_ordered_quantity
            , sum(purchasing_purchaseorderdetail.total_received_qty) as total_received_quantity
            , sum(purchasing_purchaseorderdetail.total_rejected_qty) as total_rejected_quantity
            , sum(inventory_status.inventory_quantity) as total_inventory_quantity
            , case 
                when sum(inventory_status.inventory_quantity) >= sum(purchasing_purchaseorderdetail.total_order_qty) then 'Sufficient'
                when sum(inventory_status.inventory_quantity) < sum(purchasing_purchaseorderdetail.total_order_qty) then 'Insufficient'
                else 'Unknown'
            end as inventory_coverage
        from purchasing_purchaseorderdetail
        left join inventory_status
            on purchasing_purchaseorderdetail.productid_id = inventory_status.productid_id
        group by 
            purchasing_purchaseorderdetail.productid_id
    )

select *
from product_inventory_impact
