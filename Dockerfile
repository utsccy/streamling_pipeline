FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /opt/streamling

ENV BOOTSTRAP_SERVERS=localhost:9092
ENV INPUT_TOPIC=input-events
ENV OUTPUT_TOPIC=processed-events
ENV COUNTS_TOPIC=event-counts
ENV APPLICATION_ID=streamling-input-event-processor
ENV NUM_STREAM_THREADS=1
ENV COMMIT_INTERVAL_MS=5000
ENV STATE_DIR=/tmp/kafka-streams

COPY --from=build /workspace/target/streamling-pipeline-1.0.0.jar /opt/streamling/streamling-pipeline.jar

ENTRYPOINT ["java", "-jar", "/opt/streamling/streamling-pipeline.jar"]

