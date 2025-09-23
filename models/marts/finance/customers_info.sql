with 

order_cst_ag_pymt as (
    
    select
        *
    from
        {{ref('order_customers_ag_payments')}}
),

customers as (

    select 
        *
    from 
        {{ref('stg_customers')}}
),

final as (
    
    select 
        a.customer_id,
        a.first_name,
        a.last_name,
        b.first_order_date,
        b.last_order_date,
        b.num_orders,
        b.lifetime_value
    from 
        customers as a left join order_cst_ag_pymt as b using(customer_id)
    order by a.customer_id
)

select * from final