

use warehouse kionas-worker1;

create database abc;

create schema abc.schema1;

create table abc.schema1.table1 (id int, name string);

insert into abc.schema1.table1 values (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Diana'), (5, 'Eve');
insert into abc.schema1.table1 values (1, NULL);
select * from abc.schema1.table1 order by id;

select * from abc.schema1.table1 where c2 is not null order by c1;

select * from abc.schema1.table1 where c1=13;

-- insert more 10
insert into abc.schema1.table1 values (6, 'Frank'), (7, 'Grace'), (8, 'Heidi'), (9, 'Ivan'), (10, 'Judy'), (11, 'Karl'), (12, 'Leo'), (13, 'Mallory'), (14, 'Nina'), (15, 'Oscar');

delete from abc.schema1.table1 where id = 3;

create user test_user;

create group test_group;

create role test_role;

grant role test_role to user test_user;

grant role test_role to group test_group;

delete user test_user;

delete group test_group;
delete role test_role;


select * from abc.schema1.table1 order by c1 limit 5;
select * from abc.schema1.table1 order by c1 limit 3 offset 2;
select * from abc.schema1.table1 limit 0;
select * from abc.schema1.table1 limit 5 offset 999;

-- EQUI JOIN TESTS
-- Create another table to join with
create table abc.schema1.table2 (id int, document string);
insert into abc.schema1.table2 values (1, 'Doc1'), (2, 'Doc2'), (3, 'Doc3'), (4, 'Doc4'), (5, 'Doc5');

-- Simple INNER JOIN
select t1.id, t1.name, t2.document from abc.schema1.table1 t1 join abc.schema1.table2 t2 on t1.id = t2.id where t1.id > 1 order by t1.id limit 6;

select t1.c1, t1.c2, t2.table2_c2 from abc.schema1.table1 t1 join abc.schema1.table2 t2 on t1.c1 = t2.c1 order by t1.c1;

-- group by and aggregate
select c1, count(*) from abc.schema1.table1 group by c1 order by c1;

-- group by min, max, avg
select c1, min(c2), max(c2), avg(c2) from abc.schema1.table1 group by c1 order by c1;

-- fake data
select id, name, email from bench.seed1.customers order by id limit 5;
select * from bench.seed1.products order by id limit 15;
select * from bench.seed1.orders order by id limit 5;



-- group by
select customer_id, count(*) as order_count from bench.seed1.orders group by customer_id order by order_count desc limit 5;
select c3, sum(c4) as total_quantity from bench.seed1.orders group by c2 order by c4 desc limit 5;

-- max
select customer_id, max(quantity) as max_quantity from bench.seed1.orders group by customer_id order by max_quantity desc limit 5;
select product_id, max(quantity) as max_quantity from bench.seed1.orders group by product_id order by max_quantity desc limit 5;


-- join group by
select c.name, count(*) as order_count from bench.seed1.orders o join bench.seed1.customers c on o.customer_id = c.id group by c.name order by order_count desc limit 5;    
select p.name, sum(o.quantity) as total_quantity from bench.seed1.orders o join bench.seed1.products p on o.product_id = p.id group by p.name order by total_quantity desc limit 5;

-- join group by max
select c.name, max(o.quantity) as max_quantity from bench.seed1.orders o join bench.seed1.customers c on o.customer_id = c.id group by c.name order by max_quantity desc limit 5;
select p.name, max(o.quantity) as max_quantity from bench.seed1.orders o join bench.seed1.products p on o.product_id = p.id where p.id = 15  group by p.name order by max_quantity desc limit 5;


-- create table not null test
create table abc.schema1.table3 (id int not null, name string not null, document string not null);
-- invalid insert
insert into abc.schema1.table3 values (1, 'Alice', 'Doc1'), (5, 'Eve', null);
insert into abc.schema1.table3 (id, name, document) values (1, 'Alice', 'Doc1'), (5, 'Eve', 'Doc5');

select * from abc.schema1.table3 order by id;

create table abc.schema1.table4 (id int not null, name string not null, bbb string not null);
-- invalid insert
insert into abc.schema1.table4 values (1, 'Alice', 'Doc1'), (5, 'Eve', NULL);
insert into abc.schema1.table4 (id, name, bbb) values (1, 'Alice', 'Doc1'), (5, 'Eve', 'Doc5');

select * from abc.schema1.table4 order by id;