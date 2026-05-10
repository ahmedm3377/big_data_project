# Kafka Setup

Kafka topic used by the raw producer:

```text
aircraft_states_raw
```

With this project's Docker Compose setup, the topic is created automatically by the `kafka-init` service.

To list topics manually:

```powershell
docker exec -it flight-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

To create the raw topic manually if needed:

```powershell
docker exec -it flight-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic aircraft_states_raw --partitions 1 --replication-factor 1
```

Kafka UI is available at:

```text
http://localhost:8080
```
