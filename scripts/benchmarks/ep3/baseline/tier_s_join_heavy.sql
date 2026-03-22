use warehouse kionas-worker1;
select c.id, c.name
from bench.seed_ep3_tier_s.customers c
join bench.seed_ep3_tier_s.orders o on c.id = o.customer_id;
