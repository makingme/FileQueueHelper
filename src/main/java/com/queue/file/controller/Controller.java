package com.queue.file.controller;

import com.queue.file.vo.FileQueueData;
import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public interface Controller {
    boolean open();
    long realignKey(List<Long> keyList, MVMap<Long, String> dataMap);

    void realignData() throws InterruptedException;

    long write(FileQueueData data) throws InterruptedException;

    long write(String data) throws InterruptedException;

    FileQueueData read() throws InterruptedException;

    boolean isOk();

    String getQueue();

    String getQueueName();

    String getQueuePath();
}
