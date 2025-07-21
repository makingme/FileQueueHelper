package com.queue.file.exception;

public class QueueException extends Exception{
    public QueueException() {
        super("큐 IO 중 에러 발생");
    }

    public QueueException(String message) {
        super(message);
    }

    public QueueException(Throwable cause) {
        super("큐 IO 중 에러 발생", cause);
    }

    public QueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
