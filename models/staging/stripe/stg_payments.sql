with

payments as (
    select
        ID as payment_id,
        ORDERID as order_id,
        PAYMENTMETHOD as payment_method,
        status as payment_status,
        amount as payment_amount,
        created
    from
        {{source('stripe','payments')}}
)

select * from payments