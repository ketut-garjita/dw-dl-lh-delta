create schema if not exists analytics;
create table if not exists analytics.dim_zone (
  zone_id serial primary key,
  zone_name text unique
);
create table if not exists analytics.fct_rides_daily (
  pickup_day date,
  trips int,
  total_fare numeric,
  total_tip numeric,
  avg_minutes double precision
);
