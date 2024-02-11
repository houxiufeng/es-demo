package com.example.demo.common.backoff;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

public class ExponentialBackOff implements BackOff {

    /**
     * The default initial interval.
     */
    public static final long DEFAULT_INITIAL_INTERVAL = 1000L;

    /**
     * The default multiplier.
     */
    public static final double DEFAULT_MULTIPLIER = 1.5;

    /**
     * The default maximum back off time.
     */
    public static final long DEFAULT_MAX_INTERVAL = 60000L;

    /**
     * The default maximum elapsed time.
     */
    public static final long DEFAULT_MAX_ELAPSED_TIME = 60000L * 5;

    /**
     * Enable jitter or not by default.
     */
    public static final boolean DEFAULT_ENABLE_JITTER = false;

    private static final ExponentialBackOff DEFAULT = new ExponentialBackOff();

    private final long initialInterval;
    private final double multiplier;
    private final long maxInterval;
    private final long maxElapsedTime;
    private final boolean jitterEnabled;

    private final Random random = new Random();

    private ExponentialBackOff() {
        this(DEFAULT_INITIAL_INTERVAL, DEFAULT_MULTIPLIER, DEFAULT_MAX_INTERVAL, DEFAULT_MAX_ELAPSED_TIME, DEFAULT_ENABLE_JITTER);
    }

    public ExponentialBackOff(@Nullable final Duration maxElapsedTime) {
        this(null, null, null, Optional.ofNullable(maxElapsedTime).map(Duration::toMillis).orElse(null), true);
    }

    public ExponentialBackOff(
            @Nullable final Long initialInterval,
            @Nullable final Double multiplier,
            @Nullable final Long maxInterval,
            @Nullable final Long maxElapsedTime,
            @Nullable final Boolean jitterEnabled
    ) {
        this.initialInterval = Optional.ofNullable(initialInterval).filter(internal -> internal > 0).orElse(DEFAULT_INITIAL_INTERVAL);
        this.multiplier = Optional.ofNullable(multiplier).filter(m -> m > 1).orElse(DEFAULT_MULTIPLIER);
        this.maxInterval = Optional.ofNullable(maxInterval).filter(internal -> internal > 0).orElse(DEFAULT_MAX_INTERVAL);
        this.maxElapsedTime = Optional.ofNullable(maxElapsedTime).filter(elapsed -> elapsed > 0).orElse(DEFAULT_MAX_ELAPSED_TIME);
        checkArgument(this.maxElapsedTime > this.initialInterval && this.maxElapsedTime > this.maxInterval,
                "Max elapsed time should be greater than initial and max interval!");
        this.jitterEnabled = Optional.ofNullable(jitterEnabled).orElse(DEFAULT_ENABLE_JITTER);
    }

    public static ExponentialBackOff getDefault() {
        return DEFAULT;
    }

    @Override
    public BackOffHandler start() {
        return new ExponentialBackOffHandler();
    }

    public long getInitialInterval() {
        return initialInterval;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public long getMaxInterval() {
        return maxInterval;
    }

    @Override
    public long getMaxElapsedTime() {
        return maxElapsedTime;
    }

    private class ExponentialBackOffHandler implements BackOffHandler {

        private long currentInterval = -1;
        private long currentElapsedTime = 0;

        private ExponentialBackOffHandler() {
        }

        @Override
        public long nextBackOff() {
            if (this.currentElapsedTime >= getMaxElapsedTime()) {
                return STOP;
            }
            final long nextInterval = calculateNextInterval();
            currentElapsedTime += nextInterval;
            return nextInterval;
        }

        private long calculateNextInterval() {
            final long maxInterval = getMaxInterval();
            if (this.currentInterval >= maxInterval) {
                return maxInterval;
            }
            if (this.currentInterval < 0) {
                this.currentInterval = getInitialInterval();
            } else {
                long i = this.currentInterval;
                if (jitterEnabled) {
                    i = (long) (getMultiplier() * i + random.nextFloat() * getInitialInterval());
                } else {
                    i *= getMultiplier();
                }
                this.currentInterval = Math.min(Math.max(1, i), maxInterval);
            }
            return this.currentInterval;
        }

    }

}
