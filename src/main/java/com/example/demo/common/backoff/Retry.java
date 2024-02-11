package com.example.demo.common.backoff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

public class Retry {

    private static final Logger LOGGER = LoggerFactory.getLogger(Retry.class);

    @Nonnull
    public static <T> T untilNonNull(@Nonnull final BackOff backOff, @Nonnull final TimeUnit backOffTimeUnit,
            @Nonnull final Callable<T> callable) {
        return untilSuccess(backOff, backOffTimeUnit, callable, Objects::nonNull);
    }

    public static void untilSuccess(@Nonnull final BackOff backOff, @Nonnull final TimeUnit backOffTimeUnit,
            @Nonnull final Runnable runnable) {
        checkNotNull(runnable, "Missing runnable!");
        untilSuccess(backOff, backOffTimeUnit, () -> {
            runnable.run();
            return null;
        }, ignored -> true);
    }

    public static <V> V untilSuccess(@Nonnull final BackOff backOff, @Nonnull final TimeUnit backOffTimeUnit,
            @Nonnull final Callable<V> callable, @Nonnull final Predicate<V> predicate) {
        checkNotNull(backOff, "Missing backoff!");
        checkNotNull(backOffTimeUnit, "Missing backOffTimeUnit!");
        checkNotNull(callable, "Missing callable!");
        final BackOffHandler backOffHandler = backOff.start();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final V v = callable.call();
                    if (predicate.test(v)) {
                        return v;
                    }
                    keepRetrying(backOff, backOffTimeUnit, backOffHandler, null);
                } catch (Exception e) {
                    keepRetrying(backOff, backOffTimeUnit, backOffHandler, e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryException("Retry failed eventually because current thread had been interrupted.", e);
        }
        throw new RetryException("Retry failed eventually because current thread had been interrupted.");
    }

    private static void keepRetrying(
            @Nonnull final BackOff backOff, @Nonnull final TimeUnit backOffTimeUnit, @Nonnull final BackOffHandler backOffHandler, @Nullable final Exception e
    ) throws InterruptedException {
        long nextBackOff = backOffHandler.nextBackOff();
        if (BackOffHandler.STOP == nextBackOff) {
            final long maxElapsedTime = backOff.getMaxElapsedTime();
            final String msg = String.format("Retry failed eventually after reached maximum elapsed time %s.",
                    Duration.of(maxElapsedTime, toChronoUnit(backOffTimeUnit)));
            throw new RetryException(msg, e);
        }
        LOGGER.warn("Run into error, will retry after {}", Duration.of(nextBackOff, toChronoUnit(backOffTimeUnit)), e);
        backOffTimeUnit.sleep(nextBackOff);
    }

    public static ChronoUnit toChronoUnit(TimeUnit timeUnit) {
        switch (timeUnit) {
            case NANOSECONDS:  return ChronoUnit.NANOS;
            case MICROSECONDS: return ChronoUnit.MICROS;
            case MILLISECONDS: return ChronoUnit.MILLIS;
            case SECONDS:      return ChronoUnit.SECONDS;
            case MINUTES:      return ChronoUnit.MINUTES;
            case HOURS:        return ChronoUnit.HOURS;
            case DAYS:         return ChronoUnit.DAYS;
            default: throw new AssertionError();
        }
    }

}
