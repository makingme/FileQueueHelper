package com.queue.file.exception;

public class InitializeException extends RuntimeException {
    private static final String DEFAULT_MESSAGE = "Queue initialization failed";

    public InitializeException() {
        super(DEFAULT_MESSAGE);
    }

    public InitializeException(String message) {
        super(message);
    }

    public InitializeException(Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    public InitializeException(String message, Throwable cause) {
        super(message, cause);
    }

    public InitializeException(String queueName, String detail) {
        super(String.format("queue=%s action=init error=%s", queueName, detail));
    }

    public InitializeException(String queueName, String detail, Throwable cause) {
        super(String.format("queue=%s action=init error=%s", queueName, detail), cause);
    }
}
