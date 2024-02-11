package com.example.demo.common.backoff;

public class ExponentialBackOffConfig {

    private final Long initialInterval;
    private final Double multiplier;
    private final Long maxInterval;
    private final Long maxElapsedTime;
    private final Boolean jitterEnabled;

    public ExponentialBackOffConfig(Long initialInterval, Double multiplier, Long maxInterval, Long maxElapsedTime, Boolean jitterEnabled) {
        this.initialInterval = initialInterval;
        this.multiplier = multiplier;
        this.maxInterval = maxInterval;
        this.maxElapsedTime = maxElapsedTime;
        this.jitterEnabled = jitterEnabled;
    }

    public Long getInitialInterval() {
        return initialInterval;
    }

    public Double getMultiplier() {
        return multiplier;
    }

    public Long getMaxInterval() {
        return maxInterval;
    }

    public Long getMaxElapsedTime() {
        return maxElapsedTime;
    }

    public Boolean isJitterEnabled() {
        return jitterEnabled;
    }
}
