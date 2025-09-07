-- simple staging model that expects a table/view 'rides_delta' accessible to dbt
select
  ride_id,
  pickup_ts,
  dropoff_ts,
  pu_zone,
  do_zone,
  fare::double precision as fare,
  tip::double precision as tip,
  date_trunc('day', pickup_ts) as pickup_day
from {{ source('lakehouse','rides_delta') }}
