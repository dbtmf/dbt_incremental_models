
{{
    config(
        materialized='incremental'
    )
}}

with 

orders as (

    select 
        order_id,
        count(customer_id) as num_customers
    from 
        {{ref('stg_orders')}}
    group by 1
),

payments as (
    
    select
        order_id,
        sum(payment_amount) as total_amount
    from 
        {{ref('stg_payments')}}
    group by 1
),

final as (

    select
        a.order_id,
        a.order_date,
        a.num_customers,
        b.total_amount
    from 
        orders as a left join payments as b using(order_id)
    order by 1
)

SELECT * 
FROM final
{% if is_incremental() %}
    WHERE order_date >= ( 
        SELECT MAX(order_date) 
        FROM {{ this }} 
    )
{% endif %}