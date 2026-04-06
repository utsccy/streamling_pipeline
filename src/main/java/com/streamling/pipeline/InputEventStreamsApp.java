package com.streamling.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InputEventStreamsApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(InputEventStreamsApp.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private InputEventStreamsApp() {
    }

    public static void main(String[] args) {
        Properties streamsConfig = buildStreamsConfig();
        String inputTopic = envOrDefault("INPUT_TOPIC", "input-events");
        String processedTopic = envOrDefault("OUTPUT_TOPIC", "processed-events");
        String countsTopic = envOrDefault("COUNTS_TOPIC", "event-counts");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputEvents = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> processedEvents = inputEvents
                .mapValues(InputEventStreamsApp::enrichEventOrNull)
                .filter((key, value) -> value != null);

        processedEvents.to(processedTopic, Produced.with(Serdes.String(), Serdes.String()));

        processedEvents
                .map((key, value) -> {
                    String eventType = extractEventType(value);
                    return KeyValue.pair(eventType, 1L);
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .mapValues(InputEventStreamsApp::buildCountPayload)
                .to(countsTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        streams.setStateListener((newState, oldState) ->
                LOGGER.info("Streams state changed from {} to {}", oldState, newState));

        streams.setUncaughtExceptionHandler(exception -> {
            LOGGER.error("Uncaught Kafka Streams exception", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown signal received, closing Kafka Streams application.");
            streams.close();
            shutdownLatch.countDown();
        }));

        try {
            streams.start();
            shutdownLatch.await();
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Kafka Streams application interrupted, closing.", interruptedException);
        } finally {
            streams.close();
        }
    }

    private static Properties buildStreamsConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, envOrDefault("APPLICATION_ID", "streamling-input-event-processor"));
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, envOrDefault("NUM_STREAM_THREADS", "1"));
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, envOrDefault("COMMIT_INTERVAL_MS", "5000"));
        properties.put(StreamsConfig.STATE_DIR_CONFIG, envOrDefault("STATE_DIR", "./data/kafka-streams-state"));
        return properties;
    }

    private static String enrichEventOrNull(String rawJson) {
        try {
            JsonNode event = OBJECT_MAPPER.readTree(rawJson);
            if (!isValidEvent(event)) {
                LOGGER.warn("Dropping invalid or failed event: {}", rawJson);
                return null;
            }

            ObjectNode enrichedEvent = OBJECT_MAPPER.createObjectNode();
            enrichedEvent.setAll((ObjectNode) event);
            enrichedEvent.put("processed_at", Instant.now().toString());
            enrichedEvent.put("processed_epoch_ms", Instant.now().toEpochMilli());
            enrichedEvent.put("high_value", event.path("amount").asDouble(0.0d) >= 250.0d);
            return OBJECT_MAPPER.writeValueAsString(enrichedEvent);
        } catch (JsonProcessingException jsonProcessingException) {
            LOGGER.error("Skipping malformed JSON payload: {}", rawJson, jsonProcessingException);
            return null;
        } catch (ClassCastException classCastException) {
            LOGGER.error("Skipping non-object JSON payload: {}", rawJson, classCastException);
            return null;
        }
    }

    private static boolean isValidEvent(JsonNode event) {
        return event != null
                && event.isObject()
                && !event.path("user_id").asText("").isBlank()
                && !event.path("event_type").asText("").isBlank()
                && event.path("amount").asDouble(0.0d) > 0.0d
                && !"FAILED".equalsIgnoreCase(event.path("status").asText(""));
    }

    private static String extractEventType(String rawJson) {
        try {
            JsonNode event = OBJECT_MAPPER.readTree(rawJson);
            String eventType = event.path("event_type").asText("UNKNOWN");
            return eventType.isBlank() ? "UNKNOWN" : eventType;
        } catch (JsonProcessingException jsonProcessingException) {
            LOGGER.warn("Falling back to UNKNOWN event type for payload: {}", rawJson, jsonProcessingException);
            return "UNKNOWN";
        }
    }

    private static String buildCountPayload(Long count) {
        ObjectNode payload = OBJECT_MAPPER.createObjectNode();
        payload.put("count", count);
        payload.put("updated_at", Instant.now().toString());
        return payload.toString();
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
