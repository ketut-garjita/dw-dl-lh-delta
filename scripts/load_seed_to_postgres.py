import csv, psycopg2, os
conn = psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=5432,
                        dbname=os.getenv('PGDB','dw'),
                        user=os.getenv('PGUSER','dbadmin'),
                        password=os.getenv('PGPASS','dbpassword'))
cur = conn.cursor()
cur.execute("""
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
""")
conn.commit()
# insert seeds
zones = [('Manhattan',),('Brooklyn',),('Queens',)]
cur.executemany("insert into analytics.dim_zone(zone_name) values(%s) on conflict (zone_name) do nothing", zones)
conn.commit()
print('Seeded Postgres analytics schema')
cur.close(); conn.close()
