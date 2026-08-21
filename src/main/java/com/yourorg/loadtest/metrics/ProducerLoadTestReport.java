package com.yourorg.loadtest.metrics;

import java.time.Duration;

public record ProducerLoadTestReport(
        String topic,
        Duration wallClockDuration,
        long recordsSent,
        long recordsFailed,
        long bytesSent,
        LatencyRecorder sendLatency
) {

    public double recordsPerSecond() {
        double seconds = wallClockDuration.toMillis() / 1000.0;
        return seconds > 0 ? recordsSent / seconds : 0;
    }

    public double megabytesPerSecond() {
        double seconds = wallClockDuration.toMillis() / 1000.0;
        return seconds > 0 ? (bytesSent / 1_000_000.0) / seconds : 0;
    }

    public String toSummaryString() {
        return """
                Producer load test summary
                ---------------------------------------
                Topic:               %s
                Duration:            %d s
                Records sent:        %d
                Records failed:      %d
                Throughput:          %.1f records/sec (%.2f MB/sec)
                Send latency (ms):   p50=%d  p95=%d  p99=%d  p999=%d  max=%d
                """.formatted(
                topic,
                wallClockDuration.toSeconds(),
                recordsSent,
                recordsFailed,
                recordsPerSecond(),
                megabytesPerSecond(),
                sendLatency.p50(), sendLatency.p95(), sendLatency.p99(), sendLatency.p999(), sendLatency.max()
        );
    }
}
