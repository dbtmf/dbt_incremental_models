{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            'field':'order_date',
            'data_type':'date',
            'granularity':'day'
        }
    )
}}

with 

orders as (

    select 
        order_id,
        1 as num_customers,
        order_date
    from 
        {{ref('stg_orders')}}
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
),

incrementals AS (

    SELECT * 

    FROM final

    {% if is_incremental() %}
        WHERE order_date >= ( 
            SELECT MAX(order_date) 
            FROM {{ this }} 
        )
    {% endif %}

    ORDER BY order_id
)


select * from incrementals