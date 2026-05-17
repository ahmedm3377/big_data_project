import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, from_json, lit, max, min, to_timestamp, window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False


PROJECT_DIR = Path(__file__).resolve().parents[1]

AIRCRAFT_SCHEMA = StructType(
    [
        StructField("icao24", StringType()),
        StructField("callsign", StringType()),
        StructField("origin_country", StringType()),
        StructField("time_position", LongType()),
        StructField("last_contact", LongType()),
        StructField("longitude", DoubleType()),
        StructField("latitude", DoubleType()),
        StructField("baro_altitude", DoubleType()),
        StructField("on_ground", BooleanType()),
        StructField("velocity", DoubleType()),
        StructField("true_track", DoubleType()),
        StructField("vertical_rate", DoubleType()),
        StructField("sensors", StringType()),
        StructField("geo_altitude", DoubleType()),
        StructField("squawk", StringType()),
        StructField("spi", BooleanType()),
        StructField("position_source", IntegerType()),
        StructField("api_time", LongType()),
        StructField("api_time_utc", StringType()),
        StructField("time_position_utc", StringType()),
        StructField("last_contact_utc", StringType()),
        StructField("ingested_at_utc", StringType()),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser(description="Read aircraft states from Kafka with Spark Structured Streaming.")
    parser.add_argument(
        "--mode",
        choices=["console", "country-summary", "country-summary-enriched", "latest-aircraft", "alerts"],
        default="console",
    )
    parser.add_argument("--trigger-once", action="store_true", help="Process available data once, then exit.")
    parser.add_argument("--bootstrap-servers", default=None, help="Kafka bootstrap servers.")
    parser.add_argument("--topic", default=None, help="Kafka topic to read.")
    parser.add_argument("--checkpoint-dir", default=None, help="Checkpoint directory for this streaming query.")
    parser.add_argument("--output-path", default=None, help="Output path for file-based analytics.")
    parser.add_argument("--country-metadata-path", default=None, help="HDFS/file path to country metadata CSV.")
    return parser.parse_args()


def build_spark(app_name):
    kafka_package = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", kafka_package)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def read_aircraft_stream(spark, bootstrap_servers, topic):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS json_value", "timestamp AS kafka_timestamp")
        .select("kafka_key", "kafka_timestamp", from_json(col("json_value"), AIRCRAFT_SCHEMA).alias("aircraft"))
        .select("kafka_key", "kafka_timestamp", "aircraft.*")
        .withColumn("api_event_time", to_timestamp(col("api_time_utc")))
        .withColumn("last_contact_time", to_timestamp(col("last_contact_utc")))
        .withColumn("ingested_at", to_timestamp(col("ingested_at_utc")))
        .filter(col("icao24").isNotNull())
    )

    return parsed


def build_country_summary(stream_df):
    valid = stream_df.filter(
        col("origin_country").isNotNull()
        & col("api_event_time").isNotNull()
        & col("on_ground").eqNullSafe(False)
    )

    return (
        valid.withWatermark("api_event_time", "2 minutes")
        .groupBy(window(col("api_event_time"), "1 minute"), col("origin_country"))
        .agg(
            count("*").alias("aircraft_count"),
            avg("baro_altitude").alias("avg_baro_altitude_m"),
            avg("geo_altitude").alias("avg_geo_altitude_m"),
            avg("velocity").alias("avg_velocity_mps"),
            max("velocity").alias("max_velocity_mps"),
            min("baro_altitude").alias("min_baro_altitude_m"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("origin_country"),
            col("aircraft_count"),
            col("avg_baro_altitude_m"),
            col("avg_geo_altitude_m"),
            col("avg_velocity_mps"),
            col("max_velocity_mps"),
            col("min_baro_altitude_m"),
            current_timestamp().alias("processed_at"),
        )
    )


def build_country_summary_batch(batch_df):
    valid = batch_df.filter(
        col("origin_country").isNotNull()
        & col("api_event_time").isNotNull()
        & col("on_ground").eqNullSafe(False)
    )

    return (
        valid.groupBy(window(col("api_event_time"), "1 minute"), col("origin_country"))
        .agg(
            count("*").alias("aircraft_count"),
            avg("baro_altitude").alias("avg_baro_altitude_m"),
            avg("geo_altitude").alias("avg_geo_altitude_m"),
            avg("velocity").alias("avg_velocity_mps"),
            max("velocity").alias("max_velocity_mps"),
            min("baro_altitude").alias("min_baro_altitude_m"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("origin_country"),
            col("aircraft_count"),
            col("avg_baro_altitude_m"),
            col("avg_geo_altitude_m"),
            col("avg_velocity_mps"),
            col("max_velocity_mps"),
            col("min_baro_altitude_m"),
            current_timestamp().alias("processed_at"),
        )
    )


def build_enriched_country_summary_batch(batch_df, country_metadata_path):
    batch_spark = batch_df.sparkSession
    summary_df = build_country_summary_batch(batch_df)
    metadata_df = (
        batch_spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(country_metadata_path)
    )

    summary_df.createOrReplaceTempView("country_summary_batch")
    metadata_df.createOrReplaceTempView("country_metadata")

    return batch_spark.sql(
        """
        SELECT
          s.window_start,
          s.window_end,
          s.origin_country,
          COALESCE(m.region, 'Unknown') AS region,
          COALESCE(m.continent, 'Unknown') AS continent,
          s.aircraft_count,
          s.avg_baro_altitude_m,
          s.avg_geo_altitude_m,
          s.avg_velocity_mps,
          s.max_velocity_mps,
          s.min_baro_altitude_m,
          s.processed_at
        FROM country_summary_batch s
        LEFT JOIN country_metadata m
          ON s.origin_country = m.country
        """
    )


def build_alerts(stream_df):
    airborne = stream_df.filter(col("on_ground").eqNullSafe(False))
    low_altitude = (
        airborne.filter(
            col("baro_altitude").isNotNull()
            & col("velocity").isNotNull()
            & (col("baro_altitude") < 500)
            & (col("velocity") > 70)
        )
        .withColumn("alert_type", lit("LOW_ALTITUDE"))
        .withColumn("alert_message", lit("Aircraft is airborne below 500m while moving faster than 70 m/s"))
    )

    high_speed = (
        airborne.filter(col("velocity").isNotNull() & (col("velocity") > 300))
        .withColumn("alert_type", lit("HIGH_SPEED"))
        .withColumn("alert_message", lit("Aircraft speed is above 300 m/s"))
    )

    return (
        low_altitude.unionByName(high_speed)
        .select(
            "alert_type",
            "alert_message",
            "icao24",
            "callsign",
            "origin_country",
            "longitude",
            "latitude",
            "baro_altitude",
            "geo_altitude",
            "velocity",
            "vertical_rate",
            "api_event_time",
            "last_contact_time",
            current_timestamp().alias("processed_at"),
        )
    )


def build_latest_aircraft(stream_df):
    return (
        stream_df.filter(
            col("icao24").isNotNull()
            & col("api_event_time").isNotNull()
            & col("latitude").isNotNull()
            & col("longitude").isNotNull()
        )
        .select(
            "icao24",
            "callsign",
            "origin_country",
            "longitude",
            "latitude",
            "baro_altitude",
            "geo_altitude",
            "velocity",
            "true_track",
            "vertical_rate",
            "on_ground",
            "api_event_time",
            "last_contact_time",
            "ingested_at",
            current_timestamp().alias("processed_at"),
        )
    )


def write_console(stream_df, checkpoint_dir, trigger_once=False):
    writer = (
        stream_df.writeStream.format("console")
        .option("truncate", "false")
        .option("numRows", "20")
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
    )

    if trigger_once:
        writer = writer.trigger(availableNow=True)

    return writer.start()


def write_parquet(stream_df, output_path, checkpoint_dir, trigger_once=False, output_mode="append"):
    writer = (
        stream_df.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode(output_mode)
    )

    if trigger_once:
        writer = writer.trigger(availableNow=True)

    return writer.start()


def write_country_summary_parquet(stream_df, output_path, checkpoint_dir, trigger_once=False):
    def write_batch(batch_df, batch_id):
        summary_df = build_country_summary_batch(batch_df)
        if summary_df.limit(1).count() > 0:
            summary_df.write.mode("append").parquet(output_path)
        print(f"country-summary batch_id={batch_id} written_to={output_path}")

    writer = (
        stream_df.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
    )

    if trigger_once:
        writer = writer.trigger(availableNow=True)

    return writer.start()


def write_enriched_country_summary_parquet(
    spark,
    stream_df,
    output_path,
    checkpoint_dir,
    country_metadata_path,
    trigger_once=False,
):
    def write_batch(batch_df, batch_id):
        enriched_df = build_enriched_country_summary_batch(batch_df, country_metadata_path)
        if enriched_df.limit(1).count() > 0:
            enriched_df.write.mode("append").parquet(output_path)
        print(
            f"country-summary-enriched batch_id={batch_id} "
            f"metadata={country_metadata_path} written_to={output_path}"
        )

    writer = (
        stream_df.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
    )

    if trigger_once:
        writer = writer.trigger(availableNow=True)

    return writer.start()


def main():
    args = parse_args()
    load_dotenv(PROJECT_DIR / ".env")

    app_name = os.getenv("SPARK_APP_NAME", "FlightTrackingStreaming")
    bootstrap_servers = args.bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = args.topic or os.getenv("KAFKA_RAW_TOPIC", "aircraft_states_raw")
    country_metadata_path = args.country_metadata_path or os.getenv(
        "COUNTRY_METADATA_PATH",
        "hdfs://namenode:9000/flight_tracking/static/country_metadata.csv",
    )
    checkpoint_base = Path(os.getenv("CHECKPOINT_BASE_DIR", "flight_tracking/checkpoints"))
    checkpoint_dir = Path(args.checkpoint_dir) if args.checkpoint_dir else checkpoint_base / "console_parser"

    spark = build_spark(app_name)
    spark.sparkContext.setLogLevel("WARN")

    stream_df = read_aircraft_stream(spark, bootstrap_servers, topic)

    output_base = Path(args.output_path or "/tmp/flight_tracking/output")

    if args.mode == "console":
        query = write_console(stream_df, str(checkpoint_dir), args.trigger_once)
    elif args.mode == "country-summary":
        query = write_country_summary_parquet(
            stream_df,
            str(output_base / "country_summary"),
            str(checkpoint_dir),
            args.trigger_once,
        )
    elif args.mode == "country-summary-enriched":
        query = write_enriched_country_summary_parquet(
            spark,
            stream_df,
            str(output_base / "country_summary_enriched"),
            str(checkpoint_dir),
            country_metadata_path,
            args.trigger_once,
        )
    elif args.mode == "latest-aircraft":
        latest_df = build_latest_aircraft(stream_df)
        query = write_parquet(
            latest_df,
            str(output_base / "latest_aircraft"),
            str(checkpoint_dir),
            args.trigger_once,
            output_mode="append",
        )
    elif args.mode == "alerts":
        alerts_df = build_alerts(stream_df)
        query = write_parquet(
            alerts_df,
            str(output_base / "alerts"),
            str(checkpoint_dir),
            args.trigger_once,
            output_mode="append",
        )
    else:
        raise ValueError(f"Unsupported mode: {args.mode}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
