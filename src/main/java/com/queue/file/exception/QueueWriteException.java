package com.queue.file.exception;

public class QueueWriteException extends QueueException {
    private static final String DEFAULT_MESSAGE = "Queue write failed";

    public QueueWriteException() {
        super(DEFAULT_MESSAGE);
    }

    public QueueWriteException(String message) {
        super(message);
    }

    public QueueWriteException(Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    public QueueWriteException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueueWriteException(String queueName, String detail) {
        super(queueName, "write", detail);
    }

    public QueueWriteException(String queueName, String detail, Throwable cause) {
        super(queueName, "write", detail, cause);
    }
}
