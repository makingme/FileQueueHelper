package com.queue.file.exception;

public class QueueReadException extends QueueException {
    private static final String DEFAULT_MESSAGE = "Queue read failed";

    public QueueReadException() {
        super(DEFAULT_MESSAGE);
    }

    public QueueReadException(String message) {
        super(message);
    }

    public QueueReadException(Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    public QueueReadException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueueReadException(String queueName, String detail) {
        super(queueName, "read", detail);
    }

    public QueueReadException(String queueName, String detail, Throwable cause) {
        super(queueName, "read", detail, cause);
    }
}
