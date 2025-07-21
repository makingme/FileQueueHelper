package com.queue.file.exception;

public class QueueWriteException extends QueueException{
    public QueueWriteException() {
        super("큐 쓰기 중 에러 발생");
    }

    public QueueWriteException(String message) {
        super(message);
    }

    public QueueWriteException(Throwable cause) {
        super("큐 쓰기 중 에러 발생", cause);
    }

    public QueueWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
