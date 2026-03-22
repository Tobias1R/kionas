use warehouse kionas-worker1;
select id, name from bench.seed_ep3_tier_s.customers where id > 100 order by id;
