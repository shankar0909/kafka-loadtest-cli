package com.yourorg.loadtest.engine;

/**
 * Minimal token-bucket rate limiter. Avoids pulling in Guava just for this one
 * class -- acquire() blocks (busy-wait with sleep) until a token is available.
 *
 * Not razor-precise at very high rates (thousands/sec busy-wait has some jitter),
 * but plenty accurate for load-test purposes where we care about the resulting
 * throughput distribution, not microsecond-exact pacing.
 */
public class RateLimiter {

    private final double permitsPerSecond;
    private final long intervalNanos;
    private long nextPermitAtNanos;

    private RateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        this.intervalNanos = permitsPerSecond > 0 ? (long) (1_000_000_000.0 / permitsPerSecond) : 0;
        this.nextPermitAtNanos = System.nanoTime();
    }

    public static RateLimiter create(double permitsPerSecond) {
        return new RateLimiter(permitsPerSecond);
    }

    /** Blocks until it is this caller's turn to proceed. Unlimited (no-op) if rate <= 0. */
    public synchronized void acquire() {
        if (permitsPerSecond <= 0) {
            return; // unlimited / "as fast as possible" mode
        }
        long now = System.nanoTime();
        if (nextPermitAtNanos > now) {
            long sleepNanos = nextPermitAtNanos - now;
            try {
                Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        nextPermitAtNanos = Math.max(nextPermitAtNanos, now) + intervalNanos;
    }
}
