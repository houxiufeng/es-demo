package com.example.demo;

import com.example.demo.common.backoff.ExponentialBackOff;
import com.example.demo.common.backoff.Retry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestBackoff {

    @Test
    public void testRetry() {
        AtomicInteger i = new AtomicInteger();
        Retry.untilSuccess(ExponentialBackOff.getDefault(), TimeUnit.MILLISECONDS, () -> {
            int andIncrement = i.getAndIncrement();
            System.out.println(andIncrement);
            //设置到20就会出错了
            if (i.get() < 5) {
                throw new RuntimeException("retry error");
            }
        });
    }
}
