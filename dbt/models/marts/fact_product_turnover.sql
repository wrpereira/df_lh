{{ config(materialized='table') }}

with product_inventory as (
    select
        pi.productid_id,
        p.product_nm,
        subc.subcategory_nm,
        cat.category_nm,
        pi.quantity_qt,
        p.safetystocklevel_nr,
        p.reorderpoint_tp,
        ch.standardcost_vr,
        p.listprice_vr, -- Agora buscamos o preço de venda na tabela de produtos
        (p.listprice_vr - ch.standardcost_vr) as profit_margin,
        case
            when pi.quantity_qt < p.safetystocklevel_nr then 'Baixa Rotatividade'
            when pi.quantity_qt > p.reorderpoint_tp then 'Alta Rotatividade'
            else 'Normal'
        end as stock_status,
        current_date as snapshot_date
    from {{ ref('stg_production_productinventory') }} pi
    join {{ ref('stg_production_product') }} p
        on pi.productid_id = p.productid_id
    left join {{ ref('stg_production_productsubcategory') }} subc
        on p.productsubcategoryid_id = subc.productsubcategoryid_id
    left join {{ ref('stg_production_productcategory') }} cat
        on subc.productcategoryid_id = cat.productcategoryid_id
    left join {{ ref('stg_production_productcosthistory') }} ch
        on p.productid_id = ch.productid_id
)
select *
from product_inventory
