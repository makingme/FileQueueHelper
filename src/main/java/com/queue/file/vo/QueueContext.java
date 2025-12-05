package com.queue.file.vo;

import org.h2.mvstore.MVMap;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * v2.1.0: 단일 큐의 컨텍스트 (파티션 개념 제거)
 * @since : 2025-11-28
 */
public class QueueContext {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final QueueIndexVo indexVo = new QueueIndexVo();
    private final ConcurrentSkipListSet<Long> transactionKeyList = new ConcurrentSkipListSet<>();

    private MVMap<Long, FileQueueData> dataMap;
    private MVMap<String, java.util.List<FileQueueData>> readBufferMap;
    private MVMap<String, FileQueueData> cacheMap;
    private DataSyncInfo syncInfo;

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    public QueueIndexVo getIndexVo() {
        return indexVo;
    }

    public ConcurrentSkipListSet<Long> getTransactionKeyList() {
        return transactionKeyList;
    }

    public MVMap<Long, FileQueueData> getDataMap() {
        return dataMap;
    }

    public void setDataMap(MVMap<Long, FileQueueData> dataMap) {
        this.dataMap = dataMap;
    }

    public MVMap<String, java.util.List<FileQueueData>> getReadBufferMap() {
        return readBufferMap;
    }

    public void setReadBufferMap(MVMap<String, java.util.List<FileQueueData>> readBufferMap) {
        this.readBufferMap = readBufferMap;
    }

    public MVMap<String, FileQueueData> getCacheMap() {
        return cacheMap;
    }

    public void setCacheMap(MVMap<String, FileQueueData> cacheMap) {
        this.cacheMap = cacheMap;
    }

    public DataSyncInfo getSyncInfo() {
        return syncInfo;
    }

    public void setSyncInfo(DataSyncInfo syncInfo) {
        this.syncInfo = syncInfo;
    }
}
