use warehouse compute_xl;
select customer_id, count(*) as cnt
from bench.seed_ep3_tier_m1.orders
group by customer_id;
