package com.example.demo.common.backoff;

import javax.annotation.Nullable;
import java.util.Optional;

public class FixedBackOff implements BackOff {

    /**
     * The default initial interval.
     */
    public static final long DEFAULT_INTERVAL = 1000L;

    /**
     * The default maximum elapsed time.
     */
    public static final long DEFAULT_MAX_ELAPSED_TIME = 60000L * 5;

    private static final FixedBackOff DEFAULT = new FixedBackOff();

    private final long interval;
    private final long maxElapsedTime;

    private FixedBackOff() {
        this(DEFAULT_INTERVAL, DEFAULT_MAX_ELAPSED_TIME);
    }

    public FixedBackOff(@Nullable final Long interval, @Nullable final Long maxElapsedTime) {
        this.interval = Optional.ofNullable(interval).filter(i -> i > 0).orElse(DEFAULT_INTERVAL);
        this.maxElapsedTime = Optional.ofNullable(maxElapsedTime).filter(elapsed -> elapsed > 0).orElse(DEFAULT_MAX_ELAPSED_TIME);
    }

    public static FixedBackOff getDefault() {
        return DEFAULT;
    }

    @Override
    public BackOffHandler start() {
        return new FixedBackOffHandler();
    }

    public long getInterval() {
        return interval;
    }

    @Override
    public long getMaxElapsedTime() {
        return maxElapsedTime;
    }

    private class FixedBackOffHandler implements BackOffHandler {

        private long currentElapsedTime = 0;

        private FixedBackOffHandler() {
        }

        @Override
        public long nextBackOff() {
            if (this.currentElapsedTime >= getMaxElapsedTime()) {
                return STOP;
            }
            final long nextInterval = getInterval();
            currentElapsedTime += nextInterval;
            return nextInterval;
        }

    }

}
