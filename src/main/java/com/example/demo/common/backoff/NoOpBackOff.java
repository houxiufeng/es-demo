package com.example.demo.common.backoff;

public class NoOpBackOff implements BackOff {

    private static final NoOpBackOff DEFAULT = new NoOpBackOff();

    private NoOpBackOff() {

    }

    public static NoOpBackOff getDefault() {
        return DEFAULT;
    }

    @Override
    public BackOffHandler start() {
        return new NoOpBackOffHandler();
    }

    public long getInterval() {
        return 0L;
    }

    @Override
    public long getMaxElapsedTime() {
        return 0L;
    }

    private static class NoOpBackOffHandler implements BackOffHandler {

        private NoOpBackOffHandler() {
        }

        @Override
        public long nextBackOff() {
            return STOP;
        }

    }

}
