{{ config(materialized='table') }}

with 
    payment_data as (
        select
            salesorderid_id
            , creditcardid_id
        from {{ ref('stg_sales_salesorderheader') }}
    )

    , credit_card_details as (
        select
            creditcardid_id
            , 'CREDIT CARD' as payment_method
        from {{ ref('stg_sales_creditcard') }}
    )

    , payment_method as (
        select
            payment_data.salesorderid_id
            , credit_card_details.creditcardid_id
            , coalesce(credit_card_details.payment_method, 'OTHER PAYMENT METHOD') as payment_method
        from payment_data
        left join credit_card_details
            on payment_data.creditcardid_id = credit_card_details.creditcardid_id
    )

select *
from payment_method
