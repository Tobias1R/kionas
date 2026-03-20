

use warehouse kionas-worker1;

create database abc;

create schema abc.schema1;

create table abc.schema1.table1 (id int, name string);

insert into abc.schema1.table1 values (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Diana'), (5, 'Eve');

select * from abc.schema1.table1 order by id;

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


