with    

orders as (
    select 
        *
    from 
        {{ref('stg_orders')}}
),

payments as (
    select 
        *
    from   
        {{ref('stg_payments')}}
),

final as (

    select 
        a.customer_id,
        min(a.order_date) as first_order_date,
        max(a.order_date) as last_order_date,
        count(a.order_id) as num_orders,
        sum(b.payment_amount) as lifetime_value
    from
        orders as a left join payments as b using(order_id)
    group by 1
    order by 1
)

select * from final
