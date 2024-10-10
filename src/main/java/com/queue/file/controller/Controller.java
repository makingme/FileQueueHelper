package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import com.queue.file.exception.QueueReadException;
import com.queue.file.exception.QueueWriteException;
import com.queue.file.vo.FileQueueData;
import org.h2.mvstore.MVMap;

import java.util.List;
import java.util.Map;

public interface Controller {

    boolean validate();

    boolean open();

    void close();

    long realignKey(List<Long> keyList, MVMap<Long, String> dataMap);

    void realignData() throws InitializeException;

    void write(Map<String, Object> dataMap)throws QueueWriteException;

    void write(FileQueueData data) throws QueueWriteException;

    void write(List<Map<String, Object>> dataList)throws QueueWriteException;

    void writeQueueData(List<FileQueueData> fileQueueDataList)throws QueueWriteException;

    List<FileQueueData> read(String threadName) throws QueueReadException;

    List<FileQueueData> read(String threadName, int readCount) throws QueueReadException;

    List<String> readAll();

    void removeOne() throws QueueReadException;

    void removeReadBufferOne(String threadName) throws QueueReadException;

    void clearData() throws QueueReadException;
    void clearReadBuffer() throws QueueReadException;

    default void clearAll() throws QueueReadException{
        clearData();
        clearReadBuffer();
    }

    void readCommit(String threadName) throws QueueReadException;

    boolean isOk();

    int getQueueSize();

    int getMaxSize();

    String getQueue();

    String getQueueName();

    String getQueuePath();
}
