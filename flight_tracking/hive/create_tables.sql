CREATE DATABASE IF NOT EXISTS flight_tracking;

USE flight_tracking;

CREATE EXTERNAL TABLE IF NOT EXISTS aircraft_country_summary (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  origin_country STRING,
  aircraft_count BIGINT,
  avg_baro_altitude_m DOUBLE,
  avg_geo_altitude_m DOUBLE,
  avg_velocity_mps DOUBLE,
  max_velocity_mps DOUBLE,
  min_baro_altitude_m DOUBLE,
  processed_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 'file:///workspace/flight_tracking/output/country_summary';

CREATE EXTERNAL TABLE IF NOT EXISTS aircraft_alerts (
  alert_type STRING,
  alert_message STRING,
  icao24 STRING,
  callsign STRING,
  origin_country STRING,
  longitude DOUBLE,
  latitude DOUBLE,
  baro_altitude DOUBLE,
  geo_altitude DOUBLE,
  velocity DOUBLE,
  vertical_rate DOUBLE,
  api_event_time TIMESTAMP,
  last_contact_time TIMESTAMP,
  processed_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 'file:///workspace/flight_tracking/output/alerts';

CREATE EXTERNAL TABLE IF NOT EXISTS aircraft_country_summary_enriched (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  origin_country STRING,
  region STRING,
  continent STRING,
  aircraft_count BIGINT,
  avg_baro_altitude_m DOUBLE,
  avg_geo_altitude_m DOUBLE,
  avg_velocity_mps DOUBLE,
  max_velocity_mps DOUBLE,
  min_baro_altitude_m DOUBLE,
  processed_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 'file:///workspace/flight_tracking/output/country_summary_enriched';

CREATE EXTERNAL TABLE IF NOT EXISTS latest_aircraft_states (
  icao24 STRING,
  callsign STRING,
  origin_country STRING,
  longitude DOUBLE,
  latitude DOUBLE,
  baro_altitude DOUBLE,
  geo_altitude DOUBLE,
  velocity DOUBLE,
  true_track DOUBLE,
  vertical_rate DOUBLE,
  on_ground BOOLEAN,
  api_event_time TIMESTAMP,
  last_contact_time TIMESTAMP,
  ingested_at TIMESTAMP,
  processed_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 'file:///workspace/flight_tracking/output/latest_aircraft';
