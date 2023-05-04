package com.queue.file.controller;

import com.queue.file.vo.FileQueueData;
import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public interface Controller {

    boolean validate();

    boolean open();

    void close();

    long realignKey(List<Long> keyList, MVMap<Long, String> dataMap);

    void realignData() throws InterruptedException;

    void write(Map<String, Object> dataMap)throws InterruptedException;

    void write(FileQueueData data) throws InterruptedException;

    void write(List<Map<String, Object>> dataList)throws InterruptedException;

    void writeQueueData(List<FileQueueData> fileQueueDataList)throws InterruptedException;

    List<FileQueueData> read(String threadName) throws InterruptedException;

    List<FileQueueData> read(String threadName, int readCount) throws InterruptedException;

    void readCommit(String threadName) throws InterruptedException;

    boolean isOk();

    int getQueueSize();

    int getMaxSize();

    String getQueue();

    String getQueueName();

    String getQueuePath();
}
