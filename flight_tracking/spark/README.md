# Spark Streaming

This step will read aircraft records from Kafka using Spark Structured Streaming, parse JSON, aggregate by time windows, and write processed results to Hive.

Main file to implement:

```text
flight_streaming_job.py
```

Run the first parser test from the project root using the Docker Spark container:

```powershell
docker exec flight-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /workspace/flight_tracking/spark/flight_streaming_job.py --mode console --trigger-once --bootstrap-servers kafka:29092 --checkpoint-dir /tmp/flight_tracking/checkpoints/console_parser
```

This reads from Kafka topic `aircraft_states_raw`, parses JSON records, and prints parsed aircraft rows to the console.

Run country-level windowed analytics:

```powershell
docker exec flight-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /workspace/flight_tracking/spark/flight_streaming_job.py --mode country-summary --trigger-once --bootstrap-servers kafka:29092 --checkpoint-dir /tmp/flight_tracking/checkpoints/country_summary_batch --output-path /workspace/flight_tracking/output
```

Run alert detection:

```powershell
docker exec flight-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /workspace/flight_tracking/spark/flight_streaming_job.py --mode alerts --trigger-once --bootstrap-servers kafka:29092 --checkpoint-dir /tmp/flight_tracking/checkpoints/alerts --output-path /workspace/flight_tracking/output
```

Run latest aircraft detail output:

```powershell
docker exec flight-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /workspace/flight_tracking/spark/flight_streaming_job.py --mode latest-aircraft --trigger-once --bootstrap-servers kafka:29092 --checkpoint-dir /tmp/flight_tracking/checkpoints/latest_aircraft --output-path /workspace/flight_tracking/output
```

Upload the static country metadata CSV to HDFS for the Spark SQL join:

```powershell
docker exec flight-hdfs-namenode hdfs dfs -mkdir -p /flight_tracking/static
docker cp flight_tracking/data/static/country_metadata.csv flight-hdfs-namenode:/tmp/country_metadata.csv
docker exec flight-hdfs-namenode hdfs dfs -put -f /tmp/country_metadata.csv /flight_tracking/static/country_metadata.csv
```

Run the enriched country summary join:

```powershell
docker exec flight-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /workspace/flight_tracking/spark/flight_streaming_job.py --mode country-summary-enriched --trigger-once --bootstrap-servers kafka:29092 --checkpoint-dir /tmp/flight_tracking/checkpoints/country_summary_enriched --output-path /workspace/flight_tracking/output --country-metadata-path hdfs://namenode:9000/flight_tracking/static/country_metadata.csv
```

Outputs are written as Parquet inside the Spark container:

- `/workspace/flight_tracking/output/country_summary`
- `/workspace/flight_tracking/output/country_summary_enriched`
- `/workspace/flight_tracking/output/latest_aircraft`
- `/workspace/flight_tracking/output/alerts`
