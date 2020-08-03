{{ config(materialized='table')}}

with customers as (
    select * from {{ref('stg_customers')}}
),

orders as (
    select * from {{ref('stg_orders')}}
),

customer_orders as (

    select
        O_CUSTKEY AS O_CUSTKEY,
        min(O_ORDERDATE) as first_order_date,
        max(O_ORDERDATE) as most_recent_order_date,
        count(O_ORDERKEY) as number_of_orders

    from orders

    group by 1

),


final as (

    select
        customers.C_CUSTKEY,
        customers.C_NAME,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders on customers.C_CUSTKEY = customer_orders.O_CUSTKEY

)

select * from final