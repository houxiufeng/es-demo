package com.example.demo.common.backoff;

public interface BackOff {

    BackOffHandler start();

    long getMaxElapsedTime();
}
