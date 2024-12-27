with 
    product_inventory as (
        select
            productid
            ,locationid
            ,quantity
            ,modifieddate
        from {{ source('production', 'productinventory') }}
)
,product as (
    select
        productid
        ,name as product_name
        ,productnumber
    from {{ source('production', 'product') }}
)
,location as (
    select
        locationid
        ,name as location_name
    from {{ source('production', 'location') }}
)
,final_fact_inventory as (
    select
        product_inventory.productid
        ,product.name as product_name
        ,product.productnumber
        ,product_inventory.locationid
        ,location.name as location_name
        ,sum(product_inventory.quantity) as total_quantity
        ,max(product_inventory.modifieddate) as last_inventory_update
    from product_inventory
    left join product
        on product_inventory.productid = product.productid
    left join location
        on product_inventory.locationid = location.locationid
    group by
        product_inventory.productid
        ,product.name
        ,product.productnumber
        ,product_inventory.locationid
        ,location.name
)
select * 
from final_fact_inventory;
