use warehouse compute_xl;
select id, name from bench.seed_ep3_tier_m1.customers where id > 100 order by id;
