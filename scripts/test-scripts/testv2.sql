-- 1
use warehouse compute_xl;

-- -- select count(*) from bench4.seed1.orders;

-- -- -- customers
-- -- select count(*) from bench4.seed1.customers;

-- -- -- products
-- -- select count(*) from bench4.seed1.products;

-- -- join average quantity per order
-- -- 2
-- with order_quantities as (
--     select o.id, o.quantity
--     from bench4.seed1.orders o
-- )
-- select avg(quantity) from order_quantities;

-- -- 3
-- select * from bench4.seed1.customers where id = 700;
-- -- 'Charlie Miller'
-- -- 4
-- select * from bench4.seed1.customers where name = 'Alice Clark';

-- -- 5
-- select *
--     from bench4.seed1.customers c
--     join bench4.seed1.orders o on c.id = o.customer_id
--     where c.id = 700;

-- -- join total quantity per customer named Alice
-- -- 6
-- with customer_orders as (
--     select c.name, o.quantity
--     from bench4.seed1.customers c
--     join bench4.seed1.orders o on c.id = o.customer_id
--     where c.name = 'Alice Clark'
-- )
-- select sum(quantity) from customer_orders;

-- All about Alice Clark: her orders, products, and total quantity
-- 7

with customer_orders1 as (
    select c.id, o.quantity, c.name, o.product_id
    from bench4.seed1.customers c
    join bench4.seed1.orders o on c.id = o.customer_id
    where c.id = 700
)
select * from customer_orders1 co;

with customer_orders as (
    select c.id, o.quantity, c.name, o.product_id as ppid2
    from bench4.seed1.customers c
    join bench4.seed1.orders o on c.id = o.customer_id
    where c.id = 700
)
select * from customer_orders
join bench4.seed1.products p on p.id = 8493
;

-- -- 8
-- select customer_id, sum(quantity) as total_quantity
-- from bench4.seed1.orders
-- group by customer_id
-- order by total_quantity desc
-- limit 5;

-- -- 9
-- select * from bench4.seed1.orders
-- where id in (select id from bench4.seed1.orders where quantity > 5)
-- order by quantity desc
-- limit 10;

-- /* WINDOW FUNCTIONS */
-- -- ROW_NUMBER() example: top 3 orders by quantity per customer
-- -- 10
-- with ranked_orders as (
--     select o.*, row_number() over (partition by customer_id order by quantity desc) as rn
--     from bench4.seed1.orders o
-- )
-- select *
-- from ranked_orders
-- where rn = 2
-- limit 10;

-- -- RANK() example: rank products by total quantity ordered
-- -- 11
-- with product_totals as (
--     select product_id, sum(quantity) as total_quantity
--     from bench4.seed1.orders
--     group by product_id
-- ), ranked_products as (
--     select pt.*, rank() over (order by total_quantity desc) as rnk
--     from product_totals pt
-- )
-- select *
-- from ranked_products
-- where rnk <= 5
-- limit 10;

-- -- DENSE_RANK() example: dense rank customers by total quantity ordered
-- -- 12
-- with customer_totals as (
--     select customer_id, sum(quantity) as total_quantity
--     from bench4.seed1.orders
--     group by customer_id
-- ), ranked_customers as (
--     select ct.*, dense_rank() over (order by total_quantity desc) as drnk
--     from customer_totals ct
-- )
-- select *
-- from ranked_customers
-- where drnk <= 5
-- limit 10;

-- -- count, avg, sum, min and max over
-- -- 13
-- with order_stats as (
--     select o.*, 
--         count(*) over (partition by customer_id) as order_count,
--         avg(quantity) over (partition by customer_id) as avg_quantity,
--         sum(quantity) over (partition by customer_id) as total_quantity,
--         min(quantity) over (partition by customer_id) as min_quantity,
--         max(quantity) over (partition by customer_id) as max_quantity
--     from bench4.seed1.orders o
-- )
-- select * from order_stats
-- where customer_id = 700
-- limit 10;
