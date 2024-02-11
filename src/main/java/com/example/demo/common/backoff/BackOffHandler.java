package com.example.demo.common.backoff;

public interface BackOffHandler {

    long STOP = -1;

    /**
     * Back off for a while, blocking.
     */
    long nextBackOff();

}
