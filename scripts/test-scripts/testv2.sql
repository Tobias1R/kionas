
use warehouse compute_xl;

-- select count(*) from bench4.seed1.orders;

-- -- customers
-- select count(*) from bench4.seed1.customers;

-- -- products
-- select count(*) from bench4.seed1.products;

-- join average quantity per order
with order_quantities as (
    select o.id, o.quantity
    from bench4.seed1.orders o
)
select avg(quantity) from order_quantities;

select * from bench4.seed1.customers where id = 700;
-- 'Charlie Miller'
select * from bench4.seed1.customers where name = 'Alice Clark';


select *
    from bench4.seed1.customers c
    join bench4.seed1.orders o on c.id = o.customer_id
    where c.id = 700;

-- join total quantity per customer named Alice
with customer_orders as (
    select c.name, o.quantity
    from bench4.seed1.customers c
    join bench4.seed1.orders o on c.id = o.customer_id
    where c.name = 'Alice Clark'
)
select sum(quantity) from customer_orders;

-- All about Alice Clark: her orders, products, and total quantity
with customer_orders as (
    select c.id, o.quantity, c.name, o.product_id
    from bench4.seed1.customers c
    join bench4.seed1.orders o on c.id = o.customer_id
    where c.id = 700
)
select co.*, p.name from customer_orders co
join bench4.seed1.products p on co.product_id = p.id
;

select customer_id, sum(quantity) as total_quantity
from bench4.seed1.orders
group by customer_id
order by total_quantity desc
limit 5;

select * from bench4.seed1.orders
where id in (select id from bench4.seed1.orders where quantity > 5)
order by quantity desc;