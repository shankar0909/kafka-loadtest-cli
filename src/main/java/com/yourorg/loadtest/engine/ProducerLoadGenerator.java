package com.yourorg.loadtest.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourorg.loadtest.metrics.LatencyRecorder;
import com.yourorg.loadtest.metrics.ProducerLoadTestReport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.LongAdder;

/**
 * Drives a producer send loop, emitting one JSON event per send built fresh
 * from a template (e.g. a new eventId/timestamp per event via {{uuid}} /
 * {{timestampMillis}} tokens).
 *
 * Supports two stop conditions:
 *  - a fixed number of records (use this to "drop thousands of events" precisely), or
 *  - a fixed wall-clock duration (use this for a sustained-throughput soak test).
 * Whichever limit is reached first wins if both are set.
 *
 * Sends are async (send() callback, not get()) so many requests can be in flight
 * at once -- blocking on every send would cap throughput at 1/round-trip-latency.
 */
public class ProducerLoadGenerator {

    private static final Logger log = LoggerFactory.getLogger(ProducerLoadGenerator.class);
    private final KafkaProducer<String, byte[]> producer;
    private final JsonEventFactory eventFactory;
    private final ObjectMapper objectMapper;
    private volatile boolean stopRequested = false;

    public ProducerLoadGenerator(KafkaProducer<String, byte[]> producer, JsonEventFactory eventFactory, ObjectMapper objectMapper) {
        this.producer = producer;
        this.eventFactory = eventFactory;
        this.objectMapper = objectMapper;
    }

    /**
     * @param topic                topic to send to
     * @param numRecords            total records to send; pass 0/negative to run by duration instead
     * @param duration              wall-clock cap; ignored if numRecords > 0 and reached first
     * @param targetRatePerSecond   records/sec cap; pass 0 for "as fast as possible"
     * @param keyField              top-level JSON field to use as the Kafka record key (e.g. "userId");
     *                              null/blank for a null key (round-robin/sticky partitioning)
     */
    public ProducerLoadTestReport run(String topic, long numRecords, Duration duration,
                                       double targetRatePerSecond, String keyField) {
        RateLimiter rateLimiter = RateLimiter.create(targetRatePerSecond);
        LatencyRecorder sendLatency = new LatencyRecorder();
        LongAdder sent = new LongAdder();
        LongAdder failed = new LongAdder();
        LongAdder bytesSent = new LongAdder();

        boolean byCount = numRecords > 0;
        Instant start = Instant.now();
        Instant end = byCount ? Instant.MAX : start.plus(duration);

        log.info("Starting JSON producer load test: topic={} mode={} target={} rate={}/s keyField={}",
                topic, byCount ? "count" : "duration", byCount ? numRecords : duration, targetRatePerSecond, keyField);

        long issued = 0;
        while (!stopRequested
                && (byCount ? issued < numRecords : Instant.now().isBefore(end))) {

            rateLimiter.acquire();
            long sendStartNanos = System.nanoTime();

            JsonNode event = eventFactory.newEvent();
            String key = extractKey(event, keyField);
            byte[] payload = toBytes(event);

            producer.send(new ProducerRecord<>(topic, key, payload), (metadata, exception) -> {
                if (exception != null) {
                    failed.increment();
                    log.debug("Send failed: {}", exception.getMessage());
                } else {
                    sent.increment();
                    bytesSent.add(Math.max(metadata.serializedValueSize(), 0));
                    long latencyMillis = (System.nanoTime() - sendStartNanos) / 1_000_000;
                    sendLatency.recordMillis(latencyMillis);
                }
            });
            issued++;
        }

        // Block until all in-flight/buffered sends complete so the final counts
        // and latency histogram reflect every record, not just what had returned
        // by the time the loop exited.
        producer.flush();
        producer.close(Duration.ofSeconds(10));

        Duration actualDuration = Duration.between(start, Instant.now());
        return new ProducerLoadTestReport(
                topic,
                actualDuration,
                sent.sum(),
                failed.sum(),
                bytesSent.sum(),
                sendLatency
        );
    }

    /** Allows an external caller (e.g. Ctrl+C handling) to stop an in-progress run early. */
    public void stop() {
        stopRequested = true;
    }

    private byte[] toBytes(JsonNode event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize generated JSON event", e);
        }
    }

    /** Reads keyField off the generated event as the Kafka key; null if keyField is unset or absent. */
    private String extractKey(JsonNode event, String keyField) {
        if (keyField == null || keyField.isBlank()) {
            return null;
        }
        JsonNode value = event.get(keyField);
        if (value == null) {
            // Fail fast with a clear message rather than silently sending null keys
            // for every record because of a typo'd field name.
            throw new IllegalArgumentException(
                    "--key-field '" + keyField + "' does not exist on the generated JSON event. "
                            + "Available top-level fields: " + collectFieldNames(event));
        }
        return value.asText();
    }

    private String collectFieldNames(JsonNode event) {
        StringBuilder sb = new StringBuilder();
        event.fieldNames().forEachRemaining(name -> {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(name);
        });
        return sb.toString();
    }
}
