package com.queue.file.exception;

public class QueueException extends Exception {
    private static final String DEFAULT_MESSAGE = "Queue operation failed";

    public QueueException() {
        super(DEFAULT_MESSAGE);
    }

    public QueueException(String message) {
        super(message);
    }

    public QueueException(Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    public QueueException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueueException(String queueName, String action, String detail) {
        super(String.format("queue=%s action=%s error=%s", queueName, action, detail));
    }

    public QueueException(String queueName, String action, String detail, Throwable cause) {
        super(String.format("queue=%s action=%s error=%s", queueName, action, detail), cause);
    }
}
