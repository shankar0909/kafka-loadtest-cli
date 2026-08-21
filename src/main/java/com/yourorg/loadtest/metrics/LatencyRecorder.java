package com.yourorg.loadtest.metrics;

import org.HdrHistogram.Histogram;

/**
 * Thin wrapper around HdrHistogram so callers don't need to know units/config.
 * Records values in milliseconds, tracks up to 60 seconds of range with 3
 * significant digits of precision -- plenty for poll-latency measurement.
 */
public class LatencyRecorder {

    private final Histogram histogram = new Histogram(60_000L, 3);

    public void recordMillis(long millis) {
        // Guard against occasional negative/garbage values (clock skew, GC pause
        // artifacts) so a single bad sample doesn't blow up the histogram.
        if (millis >= 0) {
            histogram.recordValue(millis);
        }
    }

    public long p50() {
        return histogram.getValueAtPercentile(50.0);
    }

    public long p95() {
        return histogram.getValueAtPercentile(95.0);
    }

    public long p99() {
        return histogram.getValueAtPercentile(99.0);
    }

    public long p999() {
        return histogram.getValueAtPercentile(99.9);
    }

    public long max() {
        return histogram.getMaxValue();
    }

    public long count() {
        return histogram.getTotalCount();
    }
}
