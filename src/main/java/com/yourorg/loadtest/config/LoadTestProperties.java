package com.yourorg.loadtest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binds the "loadtest.events" section of application.yml -- one entry per
 * event type, keyed by a name that should match the corresponding JSON
 * event template file (e.g. key "order-placed" -> events/order-placed.json).
 *
 * This is what lets `producer-test --event-type order-placed` run with zero
 * other flags: topic, record counts, rate, acks etc. all come from here.
 *
 * Example application.yml:
 *
 * loadtest:
 *   events:
 *     user-created:
 *       topic: user-created
 *       num-records: 10000
 *       rate-per-second: 2000
 *     order-placed:
 *       topic: order-events
 *       num-records: 5000
 *       rate-per-second: 1000
 *       key-field: userId
 *       acks: all
 */
@ConfigurationProperties(prefix = "loadtest")
public class LoadTestProperties {

    private Map<String, EventTypeSettings> events = new LinkedHashMap<>();

    public Map<String, EventTypeSettings> getEvents() {
        return events;
    }

    public void setEvents(Map<String, EventTypeSettings> events) {
        this.events = events;
    }

    /** Per-event-type load test settings. Every field has a sensible fallback default. */
    public static class EventTypeSettings {

        /** Required -- the topic this event type is produced to. */
        private String topic;

        /** Total records to send; takes priority over durationSeconds if > 0. */
        private long numRecords = 0;

        /** Used only if numRecords is 0. */
        private int durationSeconds = 60;

        /** Target send rate; 0 means send as fast as possible. */
        private double ratePerSecond = 1000;

        /** "1" (faster) or "all" (safer, also enables idempotent delivery). */
        private String acks = "1";

        private int lingerMs = 5;

        private int batchSizeBytes = 16384;

        /** JSON field to use as the Kafka record key, e.g. "userId". Null = null key. */
        private String keyField;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public long getNumRecords() {
            return numRecords;
        }

        public void setNumRecords(long numRecords) {
            this.numRecords = numRecords;
        }

        public int getDurationSeconds() {
            return durationSeconds;
        }

        public void setDurationSeconds(int durationSeconds) {
            this.durationSeconds = durationSeconds;
        }

        public double getRatePerSecond() {
            return ratePerSecond;
        }

        public void setRatePerSecond(double ratePerSecond) {
            this.ratePerSecond = ratePerSecond;
        }

        public String getAcks() {
            return acks;
        }

        public void setAcks(String acks) {
            this.acks = acks;
        }

        public int getLingerMs() {
            return lingerMs;
        }

        public void setLingerMs(int lingerMs) {
            this.lingerMs = lingerMs;
        }

        public int getBatchSizeBytes() {
            return batchSizeBytes;
        }

        public void setBatchSizeBytes(int batchSizeBytes) {
            this.batchSizeBytes = batchSizeBytes;
        }

        public String getKeyField() {
            return keyField;
        }

        public void setKeyField(String keyField) {
            this.keyField = keyField;
        }
    }
}
