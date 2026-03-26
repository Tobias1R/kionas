use warehouse kionas-worker1;
select customer_id, count(*) as cnt
from bench.seed_ep3_tier_s.orders
group by customer_id;
