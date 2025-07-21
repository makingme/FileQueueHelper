package com.queue.file.exception;

/**
 * @since : 2025-07-07(월)
 */
public class UnsteadyStateException extends Exception{
    public UnsteadyStateException(){
        super("비정상 상태 감지");
    }

    public UnsteadyStateException(String message) {
        super(message);
    }

    public UnsteadyStateException(Throwable cause) {
        super("비정상 상태 감지", cause);
    }

    public UnsteadyStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
