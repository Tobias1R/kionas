use warehouse kionas-worker1;

-- Phase E validation script for DATATYPES FOUNDATION.
-- The positive path below should run as-is.
-- Negative-path statements are provided commented out so you can run them one-by-one.



-- =========================================================
-- Negative validations (run one statement at a time)
-- =========================================================

-- Expected: DATETIME_TIMEZONE_NOT_ALLOWED
-- insert into dt_validation_db.foundation.temporal_decimal_events (id, event_ts, event_dt, amount, description)
-- values (10, '2024-01-05T10:00:00Z', '2024-01-05T10:00:00+02:00', '11.00', 'invalid datetime timezone');

-- Expected: TEMPORAL_LITERAL_INVALID
-- insert into dt_validation_db.foundation.temporal_decimal_events (id, event_ts, event_dt, amount, description)
-- values (11, 'not-a-timestamp', '2024-01-05 10:00:00', '11.00', 'invalid timestamp literal');

-- Expected: DECIMAL_COERCION_FAILED (scale overflow for decimal(10,2))
-- insert into dt_validation_db.foundation.temporal_decimal_events (id, event_ts, event_dt, amount, description)
-- values (12, '2024-01-05T10:00:00Z', '2024-01-05 10:00:00', '12.345', 'invalid decimal scale');

-- Expected: unsupported temporal filter literal (needs quoted temporal literal)
-- select id
-- from dt_validation_db.foundation.temporal_decimal_events
-- where event_ts >= 1704153600000;


create database if not exists dt_validation_db;
create schema if not exists dt_validation_db.foundation;

create table dt_validation_db.foundation.temporal_decimal_events (
	id int,
	event_ts timestamp,
	event_dt datetime,
	amount decimal(10,2),
	description string
);

-- Positive inserts (expected success)
insert into dt_validation_db.foundation.temporal_decimal_events (id, event_ts, event_dt, amount, description)
values
	(1, '2024-01-01T10:30:00Z', '2024-01-01 10:30:00', '10.50', 'baseline row'),
	(2, '2024-01-02T12:45:00Z', '2024-01-02 12:45:00.123456', '999.99', 'fractional datetime row'),
	(3, 1704240000000, 1704240000000, '0.01', 'epoch literals row');

-- Positive filter checks
select id, event_ts
from dt_validation_db.foundation.temporal_decimal_events
where event_ts >= '2024-01-02T00:00:00Z';

select id, event_dt
from dt_validation_db.foundation.temporal_decimal_events
where event_dt >= '2024-01-02 00:00:00';

-- Positive group-by check (temporal grouping)
select event_dt, count(*)
from dt_validation_db.foundation.temporal_decimal_events
group by event_dt
order by event_dt;

-- Positive join-key check (temporal join)
create table dt_validation_db.foundation.temporal_join_dim (
	event_ts timestamp,
	bucket string
);

insert into dt_validation_db.foundation.temporal_join_dim (event_ts, bucket)
values
	('2024-01-01T10:30:00Z', 'A'),
	('2024-01-02T12:45:00Z', 'B'),
	('2024-01-03T00:00:00Z', 'C');

select f.id, f.event_ts, d.bucket
from dt_validation_db.foundation.temporal_decimal_events f
join dt_validation_db.foundation.temporal_join_dim d
  on f.event_ts = d.event_ts
order by f.id;
