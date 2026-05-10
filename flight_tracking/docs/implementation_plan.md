# Flight Tracking Implementation Plan

## Step 1: Ingestion

Owner: Person 1

Build the OpenSky polling producer and Kafka topic setup.

Deliverables:

- `producer/opensky_producer.py`
- Kafka topic setup notes
- sample raw aircraft JSON

## Step 2: Processing and Storage

Owner: Person 2

Build the Spark Structured Streaming job and Hive tables.

Deliverables:

- `spark/flight_streaming_job.py`
- `hive/create_tables.sql`
- Spark SQL enrichment with `data/static/country_metadata.csv`

## Step 3: Dashboard and Demo

Owner: Person 3

Build the Streamlit dashboard and final demo materials.

Deliverables:

- `dashboard/flight_dashboard.py`
- dashboard screenshots
- final demo script

