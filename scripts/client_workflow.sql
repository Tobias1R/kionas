

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