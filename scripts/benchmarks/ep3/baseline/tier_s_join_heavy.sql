use warehouse compute_xl;
select c.id, c.name
from bench.seed_ep3_tier_s.customers c
join bench.seed_ep3_tier_s.orders o on c.id = o.customer_id;
