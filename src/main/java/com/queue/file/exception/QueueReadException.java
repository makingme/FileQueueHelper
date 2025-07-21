package com.queue.file.exception;

public class QueueReadException extends QueueException{
    public QueueReadException() {
        super("큐 읽기 중 에러 발생");
    }

    public QueueReadException(String message) {
        super(message);
    }

    public QueueReadException(Throwable cause) {
        super("큐 읽기 중 에러 발생", cause);
    }

    public QueueReadException(String message, Throwable cause) {
        super(message, cause);
    }
}
