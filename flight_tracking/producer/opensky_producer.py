import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer


PROJECT_DIR = Path(__file__).resolve().parents[1]

STATE_FIELDS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]

running = True


def handle_shutdown(signum, frame):
    global running
    running = False
    print("Shutdown requested. Finishing current poll before exit...")


def utc_from_epoch(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def normalize_state(raw_state, api_time):
    record = dict(zip(STATE_FIELDS, raw_state))
    record["callsign"] = record["callsign"].strip() if record.get("callsign") else None
    record["api_time"] = api_time
    record["api_time_utc"] = utc_from_epoch(api_time)
    record["time_position_utc"] = utc_from_epoch(record.get("time_position"))
    record["last_contact_utc"] = utc_from_epoch(record.get("last_contact"))
    record["ingested_at_utc"] = datetime.now(timezone.utc).isoformat()
    return record


def fetch_states(api_url, bearer_token=None):
    headers = {}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    response = requests.get(api_url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def build_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=100,
        retries=5,
    )


def publish_records(producer, topic, records):
    sent = 0
    for record in records:
        key = record.get("icao24") or "unknown"
        producer.send(topic, key=key, value=record)
        sent += 1

    producer.flush()
    return sent


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def parse_args():
    parser = argparse.ArgumentParser(description="Poll OpenSky and publish aircraft states to Kafka.")
    parser.add_argument("--once", action="store_true", help="Run one poll cycle and exit.")
    parser.add_argument("--poll-interval", type=positive_int, default=None, help="Seconds between API polls.")
    parser.add_argument("--max-records", type=positive_int, default=None, help="Maximum records to publish per poll.")
    return parser.parse_args()


def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    args = parse_args()
    load_dotenv(PROJECT_DIR / ".env")

    api_url = os.getenv("OPENSKY_API_URL", "https://opensky-network.org/api/states/all")
    bearer_token = os.getenv("OPENSKY_BEARER_TOKEN") or None
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_RAW_TOPIC", "aircraft_states_raw")
    poll_interval = args.poll_interval or int(os.getenv("POLL_INTERVAL_SECONDS", "15"))
    max_records = args.max_records or int(os.getenv("MAX_RECORDS_PER_POLL", "500"))

    producer = build_producer(bootstrap_servers)

    print(f"OpenSky API: {api_url}")
    print(f"Kafka bootstrap servers: {bootstrap_servers}")
    print(f"Kafka topic: {topic}")
    print(f"Poll interval: {poll_interval}s")
    print(f"Max records per poll: {max_records}")

    while running:
        cycle_started = time.time()
        try:
            payload = fetch_states(api_url, bearer_token)
            api_time = payload.get("time")
            states = payload.get("states") or []
            records = [normalize_state(state, api_time) for state in states[:max_records]]
            sent = publish_records(producer, topic, records)
            print(
                f"{datetime.now(timezone.utc).isoformat()} | "
                f"api_records={len(states)} | published={sent} | api_time={utc_from_epoch(api_time)}"
            )
        except Exception as exc:
            print(f"Producer error: {exc}", file=sys.stderr)

        if args.once:
            break

        elapsed = time.time() - cycle_started
        sleep_for = max(0, poll_interval - elapsed)
        time.sleep(sleep_for)

    producer.close()
    print("Producer stopped.")


if __name__ == "__main__":
    main()

