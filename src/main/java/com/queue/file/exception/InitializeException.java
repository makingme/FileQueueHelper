package com.queue.file.exception;

public class InitializeException extends Exception{
    public InitializeException(){
        super("초기화 중 에러 발생");
    }

    public InitializeException(String message) {
        super(message);
    }

    public InitializeException(Throwable cause) {
        super("초기화 중 에러 발생", cause);
    }

    public InitializeException(String message, Throwable cause) {
        super(message, cause);
    }
}
