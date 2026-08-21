package com.yourorg.loadtest.metrics;

import java.time.Duration;

public record ConsumerLoadTestReport(
        String topic,
        Duration wallClockDuration,
        long recordsConsumed,
        long bytesConsumed,
        int pollCount,
        long pollErrors,
        LatencyRecorder pollLatency
) {

    public double recordsPerSecond() {
        double seconds = wallClockDuration.toMillis() / 1000.0;
        return seconds > 0 ? recordsConsumed / seconds : 0;
    }

    public double megabytesPerSecond() {
        double seconds = wallClockDuration.toMillis() / 1000.0;
        return seconds > 0 ? (bytesConsumed / 1_000_000.0) / seconds : 0;
    }

    public String toSummaryString() {
        return """
                Consumer load test summary
                ---------------------------------------
                Topic:               %s
                Duration:            %d s
                Records consumed:    %d
                Throughput:          %.1f records/sec (%.2f MB/sec)
                Poll count:          %d
                Poll errors:         %d
                Poll latency (ms):   p50=%d  p95=%d  p99=%d  p999=%d  max=%d
                """.formatted(
                topic,
                wallClockDuration.toSeconds(),
                recordsConsumed,
                recordsPerSecond(),
                megabytesPerSecond(),
                pollCount,
                pollErrors,
                pollLatency.p50(), pollLatency.p95(), pollLatency.p99(), pollLatency.p999(), pollLatency.max()
        );
    }
}
