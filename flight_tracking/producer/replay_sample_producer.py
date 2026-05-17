import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from opensky_producer import build_producer, publish_records


PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SAMPLE_FILE = PROJECT_DIR / "data" / "sample" / "opensky_normalized_sample.jsonl"

running = True


def handle_shutdown(signum, frame):
    global running
    running = False
    print("Shutdown requested. Stopping replay after the current record...")


def non_negative_float(value):
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be zero or greater")
    return parsed


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def load_jsonl(path):
    if not path.exists():
        raise FileNotFoundError(f"Sample file not found: {path}")

    records = []
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number} in {path}") from exc

    if not records:
        raise ValueError(f"No replay records found in {path}")
    return records


def refresh_record_timestamps(record):
    refreshed = record.copy()
    now = datetime.now(timezone.utc)
    now_epoch = int(now.timestamp())
    now_iso = now.isoformat()

    refreshed["api_time"] = now_epoch
    refreshed["api_time_utc"] = now_iso
    refreshed["time_position"] = now_epoch
    refreshed["time_position_utc"] = now_iso
    refreshed["last_contact"] = now_epoch
    refreshed["last_contact_utc"] = now_iso
    refreshed["ingested_at_utc"] = now_iso
    return refreshed


def parse_args():
    parser = argparse.ArgumentParser(description="Replay saved OpenSky sample records into Kafka.")
    parser.add_argument("--sample-file", type=Path, default=DEFAULT_SAMPLE_FILE, help="JSONL file to replay.")
    parser.add_argument("--delay", type=non_negative_float, default=0.25, help="Seconds to wait between records.")
    parser.add_argument("--loop", action="store_true", help="Keep replaying the sample until stopped.")
    parser.add_argument("--max-records", type=positive_int, default=None, help="Maximum records to publish before exiting.")
    parser.add_argument(
        "--keep-original-timestamps",
        action="store_true",
        help="Do not replace sample event timestamps with current UTC time.",
    )
    return parser.parse_args()


def main():
    global running

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    args = parse_args()
    load_dotenv(PROJECT_DIR / ".env")

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_RAW_TOPIC", "aircraft_states_raw")
    records = load_jsonl(args.sample_file)
    producer = build_producer(bootstrap_servers)

    print(f"Replay sample: {args.sample_file}")
    print(f"Kafka bootstrap servers: {bootstrap_servers}")
    print(f"Kafka topic: {topic}")
    print(f"Records in sample: {len(records)}")

    published = 0
    try:
        while running:
            for record in records:
                if not running:
                    break
                if args.max_records is not None and published >= args.max_records:
                    running = False
                    break

                message = record if args.keep_original_timestamps else refresh_record_timestamps(record)
                published += publish_records(producer, topic, [message])
                print(
                    f"{datetime.now(timezone.utc).isoformat()} | "
                    f"published={published} | icao24={message.get('icao24')} | callsign={message.get('callsign')}"
                )
                time.sleep(args.delay)

            if not args.loop:
                break
    except Exception as exc:
        print(f"Replay producer error: {exc}", file=sys.stderr)
        raise
    finally:
        producer.close()
        print(f"Replay producer stopped. Total published: {published}")


if __name__ == "__main__":
    main()
