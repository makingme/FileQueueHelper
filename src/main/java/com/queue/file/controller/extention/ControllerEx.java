package com.queue.file.controller.extention;

import com.google.gson.Gson;
import com.queue.file.exception.QueueReadException;
import com.queue.file.exception.QueueWriteException;
import com.queue.file.vo.extention.FileQueueDataEx;
import org.apache.commons.lang3.ObjectUtils;
import org.h2.mvstore.MVMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface ControllerEx {
    Gson gson = new Gson();
    
    // 큐 검증
    boolean validate();
    
    // 큐 오픈
    boolean open();
    
    // 데이터 키 재정렬
    long realignKey(List<Long> keyList, MVMap<Long, String> dataMap);
    
    //상태 체크
    boolean isOk();
    
    // 단건 쓰기
    default void write(Object dataObject) throws QueueWriteException{
        write("", dataObject);
    }
    default void write(String tag, Object dataObject) throws QueueWriteException{
        write(tag, "", dataObject);
    }
    default void write(String tag, String partition, Object dataObject) throws QueueWriteException{
        if(ObjectUtils.isEmpty(dataObject)){
            return;
        }
        String bodyData = gson.toJson(dataObject);
        String storeData =
                partition + FileQueueDataEx.DELIMITER +
                        tag + FileQueueDataEx.DELIMITER +
                        bodyData + FileQueueDataEx.DELIMITER +
                        System.currentTimeMillis() + FileQueueDataEx.DELIMITER;
        writeQueueData(Collections.singletonList(storeData));
    }

    // 대량 쓰기
    default void writeBulk(List<Object> dataList) throws QueueWriteException{
        writeBulk("", dataList);
    }
    default void writeBulk(String tag, List<Object> dataList) throws QueueWriteException{
        writeBulk(tag, "", dataList);
    }
    default void writeBulk(String tag, String partition, List<Object> dataList) throws QueueWriteException{
        if(ObjectUtils.isEmpty(dataList)){
            return;
        }

        List<String> storeDataList = new ArrayList<>(dataList.size());
        long now = System.currentTimeMillis();
        for(Object object : dataList){
            String bodyData = gson.toJson(object);
            String storeData =
                    partition + FileQueueDataEx.DELIMITER +
                            tag + FileQueueDataEx.DELIMITER +
                            bodyData + FileQueueDataEx.DELIMITER +
                            now + FileQueueDataEx.DELIMITER;
            storeDataList.add(storeData);
        }
        writeQueueData(storeDataList);
    }
    void writeQueueData(List<String> storeDataList) throws QueueWriteException;

    // 읽기
    default List<FileQueueDataEx> read() throws QueueReadException {
        return read(Thread.currentThread().getName(),1);
    }
    default List<FileQueueDataEx> read(String threadName) throws QueueReadException{
        return read(threadName,1);
    }
    List<FileQueueDataEx> read(String threadName, int requestCount) throws QueueReadException;
    void readCommit(String threadName) throws QueueReadException;

    List<String> allData();
    List<List<FileQueueDataEx>> allReadBuffer();
    FileQueueDataEx removeOne() throws QueueReadException;
    List<FileQueueDataEx> removeReadBufferOne(String threadName) throws QueueReadException;
    void clear() throws QueueReadException;
    void close();
    void commit();
}
