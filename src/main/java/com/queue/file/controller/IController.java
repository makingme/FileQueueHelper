package com.queue.file.controller;

import com.queue.file.exception.QueueReadException;
import com.queue.file.vo.FileQueueData;

import java.util.List;

public interface IController {
    static final String DEFAULT_PARTITION = "DP201019";

    List<String> allData();
    List<List<FileQueueData>> allReadBuffer();
    FileQueueData removeOne() throws QueueReadException;
    List<FileQueueData> removeReadBufferOne(String threadName) throws QueueReadException;
    void clear() throws QueueReadException;
    void close();
    void commit();
}
