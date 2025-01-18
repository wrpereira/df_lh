with product_inventory as (
    select
         production_productinventory.productid_id
        ,production_product.product_nm
        ,production_productsubcategory.subcategory_nm
        ,production_productcategory.category_nm
        ,production_productinventory.quantity_qt
        ,production_product.safetystocklevel_nr
        ,production_product.reorderpoint_tp
        ,production_productcosthistory.standardcost_vr
        ,production_product.listprice_vr 
        ,round((production_product.listprice_vr - production_productcosthistory.standardcost_vr), 2) as profit_margin
        ,case
            when production_productinventory.quantity_qt < production_product.safetystocklevel_nr then 'Low Turnover'
            when production_productinventory.quantity_qt > production_product.reorderpoint_tp then 'High Turnover'
            else 'Normal Turnover'
        end as stock_status
        ,sum(fact_sales.total_quantity) as total_quantity_sold 
        ,sum(fact_sales.total_sales_value) as total_sales_value 
        ,current_date as snapshot_date
    from {{ ref('stg_production_productinventory') }} as production_productinventory
    join {{ ref('stg_production_product') }} as production_product
        on production_productinventory.productid_id = production_product.productid_id
    left join {{ ref('stg_production_productsubcategory') }} as production_productsubcategory
        on production_product.productsubcategoryid_id = production_productsubcategory.productsubcategoryid_id
    left join {{ ref('stg_production_productcategory') }} as production_productcategory
        on production_productsubcategory.productcategoryid_id = production_productcategory.productcategoryid_id
    left join {{ ref('stg_production_productcosthistory') }} as production_productcosthistory
        on production_product.productid_id = production_productcosthistory.productid_id
    left join {{ ref('fact_sales') }} as fact_sales
        on production_product.productid_id = fact_sales.productid_id
    group by 
         production_productinventory.productid_id
        ,production_product.product_nm
        ,production_productsubcategory.subcategory_nm
        ,production_productcategory.category_nm
        ,production_productinventory.quantity_qt
        ,production_product.safetystocklevel_nr
        ,production_product.reorderpoint_tp
        ,production_productcosthistory.standardcost_vr
        ,production_product.listprice_vr
)

select *,
       round(total_quantity_sold / nullif(quantity_qt, 0), 2) as turnover_rate 
from product_inventory