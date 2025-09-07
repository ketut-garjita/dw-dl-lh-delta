select
  pickup_day,
  count(*) as trips,
  sum(fare) as total_fare,
  sum(tip) as total_tip,
  avg(extract(epoch from (dropoff_ts - pickup_ts))/60.0) as avg_minutes
from {{ ref('stg_rides') }}
group by 1
