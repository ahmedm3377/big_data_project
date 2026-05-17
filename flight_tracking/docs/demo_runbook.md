# Flight Tracking Demo Runbook

Use this checklist when recording the project demo. The most reliable path is to replay the saved sample data, because it keeps the dashboard moving even if the OpenSky API is slow or temporarily unavailable.

## 1. Start Infrastructure

```powershell
docker compose up -d
```

Check the UIs:

- Kafka UI: `http://localhost:8080`
- Spark UI: `http://localhost:8081`
- HDFS UI: `http://localhost:9870`

## 2. Prepare Hive and Static Data

```powershell
docker exec flight-hdfs-namenode hdfs dfs -mkdir -p /flight_tracking/static
docker cp flight_tracking/data/static/country_metadata.csv flight-hdfs-namenode:/tmp/country_metadata.csv
docker exec flight-hdfs-namenode hdfs dfs -put -f /tmp/country_metadata.csv /flight_tracking/static/country_metadata.csv
docker exec flight-hive beeline -u jdbc:hive2://localhost:10000 -f /workspace/flight_tracking/hive/create_tables.sql
```

## 3. Produce Flight Events

Stable replay mode:

```powershell
.\.venv\Scripts\python.exe flight_tracking\producer\replay_sample_producer.py --loop --delay 0.25
```

Live OpenSky mode:

```powershell
.\.venv\Scripts\python.exe flight_tracking\producer\opensky_producer.py --poll-interval 15 --max-records 100
```

## 4. Process the Stream

For a simple recording, run these after the producer has sent records:

```powershell
docker exec flight-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /workspace/flight_tracking/spark/flight_streaming_job.py --mode country-summary-enriched --trigger-once --bootstrap-servers kafka:29092 --checkpoint-dir /tmp/flight_tracking/checkpoints/country_summary_enriched_v2 --output-path /workspace/flight_tracking/output --country-metadata-path hdfs://namenode:9000/flight_tracking/static/country_metadata.csv
docker exec flight-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /workspace/flight_tracking/spark/flight_streaming_job.py --mode latest-aircraft --trigger-once --bootstrap-servers kafka:29092 --checkpoint-dir /tmp/flight_tracking/checkpoints/latest_aircraft --output-path /workspace/flight_tracking/output
docker exec flight-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /workspace/flight_tracking/spark/flight_streaming_job.py --mode alerts --trigger-once --bootstrap-servers kafka:29092 --checkpoint-dir /tmp/flight_tracking/checkpoints/alerts --output-path /workspace/flight_tracking/output
```

For a more live-looking demo, remove `--trigger-once` and keep each Spark command running in its own terminal while the producer runs.

## 5. Open the Dashboard

```powershell
.\.venv\Scripts\streamlit.exe run flight_tracking\dashboard\flight_dashboard.py
```

Open `http://localhost:8501`.

Suggested recording order:

1. Show Kafka UI topic messages in `aircraft_states_raw`.
2. Show Spark UI while jobs process the stream.
3. Show Hive tables with enriched country and detailed aircraft data.
4. Show Streamlit dashboard tabs: Overview, Country Analytics, Aircraft Map, Latest Aircraft, Alerts.

## Useful Hive Queries

```powershell
docker exec flight-hive beeline -u jdbc:hive2://localhost:10000 -e "SELECT origin_country, region, continent, aircraft_count, ROUND(avg_velocity_mps, 2) AS avg_speed FROM flight_tracking.aircraft_country_summary_enriched ORDER BY window_start DESC, aircraft_count DESC LIMIT 10;"
docker exec flight-hive beeline -u jdbc:hive2://localhost:10000 -e "SELECT callsign, origin_country, ROUND(latitude, 4) AS lat, ROUND(longitude, 4) AS lon, ROUND(velocity, 2) AS speed FROM flight_tracking.latest_aircraft_states ORDER BY api_event_time DESC LIMIT 10;"
docker exec flight-hive beeline -u jdbc:hive2://localhost:10000 -e "SELECT alert_type, callsign, origin_country, ROUND(baro_altitude, 2) AS altitude_m, ROUND(velocity, 2) AS speed FROM flight_tracking.aircraft_alerts ORDER BY api_event_time DESC LIMIT 10;"
```
