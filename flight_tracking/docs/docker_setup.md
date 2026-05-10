# Docker Setup

The project uses Docker Compose for infrastructure services and the local Python virtual environment for development.

## Why Compose Instead of One Container

Kafka, Spark, Hive, and HDFS are separate services. Running them as separate containers makes ports, logs, restarts, and debugging much clearer for a team project.

## Services

- `zookeeper` - Kafka coordination service.
- `kafka` - Message broker for streaming aircraft records.
- `kafka-init` - One-time topic creation.
- `kafka-ui` - Web UI for checking topics and messages.
- `spark-master` - Spark cluster master.
- `spark-worker` - Spark worker node.
- `namenode` - HDFS NameNode for the bonus static dataset.
- `datanode` - HDFS DataNode.
- `hive` - HiveServer2 for persistent storage.

## Start Services

From the project root:

```powershell
docker compose up -d
```

## Check Services

```powershell
docker compose ps
docker exec -it flight-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Useful URLs:

- Kafka UI: `http://localhost:8080`
- Spark master: `http://localhost:8081`
- Spark worker: `http://localhost:8082`
- HDFS NameNode: `http://localhost:9870`
- HiveServer2 UI: `http://localhost:10002`

## Stop Services

```powershell
docker compose down
```

To remove persistent Docker volumes as well:

```powershell
docker compose down -v
```

