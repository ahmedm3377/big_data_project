import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv


PROJECT_DIR = Path(__file__).resolve().parents[1]
SAMPLE_DIR = PROJECT_DIR / "data" / "sample"

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


def save_samples(payload, normalized_records):
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    raw_path = SAMPLE_DIR / "opensky_raw_sample.json"
    normalized_path = SAMPLE_DIR / "opensky_normalized_sample.jsonl"

    with raw_path.open("w", encoding="utf-8") as raw_file:
        json.dump(payload, raw_file, indent=2)

    with normalized_path.open("w", encoding="utf-8") as normalized_file:
        for record in normalized_records:
            normalized_file.write(json.dumps(record) + "\n")

    return raw_path, normalized_path


def main():
    parser = argparse.ArgumentParser(description="Test OpenSky API access and save sample records.")
    parser.add_argument("--limit", type=int, default=25, help="Number of normalized sample records to save.")
    args = parser.parse_args()

    load_dotenv(PROJECT_DIR / ".env")

    api_url = os.getenv("OPENSKY_API_URL", "https://opensky-network.org/api/states/all")
    bearer_token = os.getenv("OPENSKY_BEARER_TOKEN") or None

    payload = fetch_states(api_url, bearer_token)
    states = payload.get("states") or []
    api_time = payload.get("time")
    normalized = [normalize_state(state, api_time) for state in states[: args.limit]]
    raw_path, normalized_path = save_samples(payload, normalized)

    print(f"OpenSky status: OK")
    print(f"API time: {api_time} ({utc_from_epoch(api_time)})")
    print(f"Aircraft records returned: {len(states)}")
    print(f"Normalized records saved: {len(normalized)}")
    print(f"Raw sample: {raw_path}")
    print(f"Normalized sample: {normalized_path}")

    if normalized:
        print("First normalized record:")
        print(json.dumps(normalized[0], indent=2))


if __name__ == "__main__":
    main()

