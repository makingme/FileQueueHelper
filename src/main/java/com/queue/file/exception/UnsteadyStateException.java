package com.queue.file.exception;

/**
 * @since : 2025-07-07
 */
public class UnsteadyStateException extends Exception {
    private static final String DEFAULT_MESSAGE = "Queue is in an unsteady state";

    public UnsteadyStateException() {
        super(DEFAULT_MESSAGE);
    }

    public UnsteadyStateException(String message) {
        super(message);
    }

    public UnsteadyStateException(Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    public UnsteadyStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsteadyStateException(String queueName, String detail) {
        super(String.format("queue=%s action=state-check error=%s", queueName, detail));
    }

    public UnsteadyStateException(String queueName, String detail, Throwable cause) {
        super(String.format("queue=%s action=state-check error=%s", queueName, detail), cause);
    }
}
