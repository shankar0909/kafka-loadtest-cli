package com.yourorg.loadtest.engine;

import com.yourorg.loadtest.metrics.ConsumerLoadTestReport;
import com.yourorg.loadtest.metrics.LatencyRecorder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * Runs a poll loop against a topic for a fixed wall-clock duration and records
 * throughput + per-poll latency. Designed to be driven from a Spring Shell
 * command, but has no Spring dependency itself so it's independently testable.
 */
public class ConsumerLoadGenerator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLoadGenerator.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private volatile boolean stopRequested = false;

    public ConsumerLoadGenerator(KafkaConsumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    /**
     * @param topic          topic to subscribe to
     * @param duration       wall-clock time to run the poll loop for
     * @param pollTimeout    per-poll() timeout -- how long poll() blocks waiting for records
     * @param warmupSeconds  discard metrics from this initial window (JIT/connection warmup skew)
     */
    public ConsumerLoadTestReport run(String topic, Duration duration, Duration pollTimeout, int warmupSeconds) {
        consumer.subscribe(java.util.List.of(topic));
        log.info("Subscribed to topic '{}', running for {}s (warmup {}s)", topic, duration.toSeconds(), warmupSeconds);

        LatencyRecorder pollLatency = new LatencyRecorder();
        long recordsConsumed = 0;
        long bytesConsumed = 0;
        int pollCount = 0;
        long pollErrors = 0;

        Instant start = Instant.now();
        Instant warmupEnd = start.plusSeconds(warmupSeconds);
        Instant end = start.plus(duration);

        try {
            while (!stopRequested && Instant.now().isBefore(end)) {
                long pollStart = System.nanoTime();
                ConsumerRecords<byte[], byte[]> records;
                try {
                    records = consumer.poll(pollTimeout);
                } catch (WakeupException we) {
                    break;
                } catch (Exception e) {
                    pollErrors++;
                    log.warn("poll() failed: {}", e.getMessage());
                    continue;
                }
                long pollMillis = (System.nanoTime() - pollStart) / 1_000_000;

                boolean pastWarmup = Instant.now().isAfter(warmupEnd);
                if (pastWarmup) {
                    pollLatency.recordMillis(pollMillis);
                    pollCount++;
                }

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (pastWarmup) {
                        recordsConsumed++;
                        bytesConsumed += (record.value() != null ? record.value().length : 0)
                                + (record.key() != null ? record.key().length : 0);
                    }
                }
            }
        } finally {
            consumer.close(Duration.ofSeconds(5));
        }

        Duration actualDuration = Duration.between(warmupEnd.isAfter(Instant.now()) ? start : warmupEnd, Instant.now());
        return new ConsumerLoadTestReport(
                topic,
                actualDuration,
                recordsConsumed,
                bytesConsumed,
                pollCount,
                pollErrors,
                pollLatency
        );
    }

    /** Allows an external caller (e.g. a shell command handling Ctrl+C) to stop early. */
    public void stop() {
        stopRequested = true;
        consumer.wakeup();
    }
}
