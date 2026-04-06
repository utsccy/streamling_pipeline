# Streamling Pipeline

This project builds a complete Confluent Platform sample pipeline:

1. Confluent Datagen produces synthetic transaction-like events to `input-events` at 10 messages per second.
2. A Java Kafka Streams application filters out failed or invalid events, enriches the remaining records with processing metadata, and publishes them to `processed-events`.
3. The same Streams application also maintains per-event-type running counts in `event-counts`.
4. A Kafka Connect file sink writes the processed JSON payloads to `data/output/processed-events.jsonl`.

## Project layout

```text
.
|-- config
|   |-- connect-standalone.properties
|   |-- datagen.properties
|   `-- file-sink.properties
|-- scripts
|   |-- create-topics.ps1
|   `-- create-topics.sh
|-- src/main/java/com/streamling/pipeline/InputEventStreamsApp.java
|-- Dockerfile
`-- pom.xml
```

## What the pipeline does

### Topic design

The sample uses three topics:

- `input-events`
  - Partitions: `3`
  - Replication factor: `1` for local/dev Confluent Platform environments
  - Retention: `7 days`
- `processed-events`
  - Partitions: `3`
  - Replication factor: `1`
  - Retention: `7 days`
- `event-counts`
  - Partitions: `1`
  - Replication factor: `1`
  - Cleanup policy: `compact,delete` for rolling aggregate snapshots

### Synthetic data model

The Datagen connector emits realistic transaction and user activity style records with fields such as:

- `event_id`
- `user_id`
- `event_type`
- `merchant`
- `amount`
- `currency`
- `channel`
- `region`
- `status`
- `card_type`
- `loyalty_tier`

## Prerequisites

- Confluent Platform running locally and reachable at `localhost:9092`
- Kafka Connect standalone available on the machine where you run the connectors
- Confluent Datagen installed in Connect:

```bash
confluent-hub install confluentinc/kafka-connect-datagen:latest
```

- Java 17+
- Maven 3.9+

If you are running Kafka Connect in Docker or a non-default installation, adjust `plugin.path` in [config/connect-standalone.properties](/c:/Users/utscc/OneDrive/Documents/GitHub/streamling_pipeline/config/connect-standalone.properties) so it includes your Confluent Hub components directory and the built-in filestream connector directory.

## Setup and run

### 1. Create the Kafka topics

The topic scripts are Docker-based, so they run `kafka-topics` from a Confluent image instead of requiring a local Kafka CLI install.

Supported environment variables:

- `BOOTSTRAP_SERVERS`
  - Default: `host.docker.internal:9092`
  - Use this when your Kafka broker is running on your host machine and the topic script runs in Docker.
- `DOCKER_NETWORK`
  - Optional Docker network name to join when your Kafka broker is running in Docker.
- `KAFKA_IMAGE`
  - Optional Kafka image override. Default: `confluentinc/cp-kafka:7.7.0`
- `REPLICATION_FACTOR`
  - Default: `1`

If your broker is running in Docker, set both `DOCKER_NETWORK` and a broker address that is resolvable inside that network, for example `broker:29092`.

Linux/macOS:

```bash
chmod +x scripts/create-topics.sh
./scripts/create-topics.sh
```

PowerShell:

```powershell
./scripts/create-topics.ps1
```

Example when Kafka is running on your host machine:

```powershell
$env:BOOTSTRAP_SERVERS = 'host.docker.internal:9092'
./scripts/create-topics.ps1
```

Example when Kafka is running in Docker Compose:

```powershell
$env:DOCKER_NETWORK = 'confluent'
$env:BOOTSTRAP_SERVERS = 'broker:29092'
./scripts/create-topics.ps1
```

### 2. Build the Kafka Streams application

If you have multiple JDKs installed, point `JAVA_HOME` to JDK 17 before building.

```bash
mvn clean package
```

The shaded application JAR will be created at `target/streamling-pipeline-1.0.0.jar`.

### 3. Start the Kafka Streams processor

```bash
java -jar target/streamling-pipeline-1.0.0.jar
```

Optional environment overrides:

```bash
BOOTSTRAP_SERVERS=localhost:9092
INPUT_TOPIC=input-events
OUTPUT_TOPIC=processed-events
COUNTS_TOPIC=event-counts
APPLICATION_ID=streamling-input-event-processor
```

### 4. Start Kafka Connect standalone with Datagen and the file sink

Create the output directory first if it does not already exist:

```bash
mkdir -p data/output
```

Then start both connectors in the same standalone worker:

```bash
connect-standalone config/connect-standalone.properties config/datagen.properties config/file-sink.properties
```

Run this command from the repository root so the relative paths in the worker and sink configurations resolve correctly.

This worker will:

- generate `input-events`
- read `processed-events`
- append each processed JSON document to `data/output/processed-events.jsonl`

## Verifying the pipeline

Inspect the input topic:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic input-events \
  --from-beginning \
  --property print.key=true
```

Inspect the transformed topic:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic processed-events \
  --from-beginning \
  --property print.key=true
```

Inspect the running aggregates:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic event-counts \
  --from-beginning \
  --property print.key=true
```

Inspect the sink output file:

```bash
tail -f data/output/processed-events.jsonl
```

PowerShell alternative:

```powershell
Get-Content .\data\output\processed-events.jsonl -Wait
```

## Error handling

### Datagen and Connect

- `errors.tolerance=all` keeps the standalone worker alive if an individual record fails.
- `errors.log.enable=true` and `errors.log.include.messages=true` log failing records for troubleshooting.

### Kafka Streams

- `LogAndContinueExceptionHandler` prevents a single deserialization problem from killing the pipeline.
- Malformed JSON is logged and skipped.
- Events with missing required fields, non-positive amounts, or `status=FAILED` are filtered out.
- An uncaught exception handler shuts down the client cleanly on unrecoverable stream thread failures.
- A JVM shutdown hook closes the Streams application gracefully.

## Containerization

The included [Dockerfile](/c:/Users/utscc/OneDrive/Documents/GitHub/streamling_pipeline/Dockerfile) packages the Kafka Streams application into a runnable image.

Build the image:

```bash
docker build -t streamling-pipeline:local .
```

Run the image against a local Confluent Platform broker:

```bash
docker run --rm \
  --network host \
  -e BOOTSTRAP_SERVERS=localhost:9092 \
  -e INPUT_TOPIC=input-events \
  -e OUTPUT_TOPIC=processed-events \
  -e COUNTS_TOPIC=event-counts \
  streamling-pipeline:local
```

If your Docker environment does not support `--network host`, connect the container to the same Docker network as your Kafka broker and set `BOOTSTRAP_SERVERS` accordingly.

## Notes

- The sample uses replication factor `1` because it is intended for local or single-broker development environments. Increase it for multi-broker clusters.
- If you run on Confluent Platform in Docker, mount this repository into the Connect container and update file paths as needed.
- FileStream connectors are intended for demos and development workflows, not production persistence.
