# Producer

This step will poll the OpenSky API, normalize each aircraft state into JSON, and send the records to Kafka.

Main file to implement:

```text
opensky_producer.py
```

Run one test poll and publish records to Kafka:

```powershell
.\.venv\Scripts\python.exe flight_tracking\producer\opensky_producer.py --once --max-records 25
```

Run continuously:

```powershell
.\.venv\Scripts\python.exe flight_tracking\producer\opensky_producer.py
```

Replay saved sample records into Kafka for a stable demo:

```powershell
.\.venv\Scripts\python.exe flight_tracking\producer\replay_sample_producer.py --loop --delay 0.25
```

For a short replay test:

```powershell
.\.venv\Scripts\python.exe flight_tracking\producer\replay_sample_producer.py --max-records 25 --delay 0
```

API test file:

```text
test_opensky_api.py
```

Run from the project root:

```powershell
.\.venv\Scripts\python.exe flight_tracking\producer\test_opensky_api.py --limit 25
```
