package com.queue.file.vo;

import org.h2.mvstore.MVMap;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @since : 2025-07-16(수)
 */
public class PartitionContext {
    private final String partitionName;
    private MVMap<Long, FileQueueData> dataMap;
    private MVMap<String,  List<FileQueueData>> readBufferMap;
    private MVMap<String, Object> cacheMap;
    private ConcurrentSkipListSet<Long> transactionKeyList = new ConcurrentSkipListSet<>();
    private final PartitionIndexVo indexVo = new PartitionIndexVo();
    private DataSyncInfo syncInfo;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(); // 파티션별 락 추가

    public PartitionContext(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getPartitionName() { return partitionName; }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    public MVMap<Long, FileQueueData> getDataMap() {
        return dataMap;
    }

    public void setDataMap(MVMap<Long, FileQueueData> dataMap) {
        this.dataMap = dataMap;
    }

    public MVMap<String, List<FileQueueData>> getReadBufferMap() {
        return readBufferMap;
    }

    public void setReadBufferMap(MVMap<String, List<FileQueueData>> readBufferMap) {
        this.readBufferMap = readBufferMap;
    }

    public MVMap<String, Object> getCacheMap() {
        return cacheMap;
    }

    public void setCacheMap(MVMap<String, Object> cacheMap) {
        this.cacheMap = cacheMap;
    }

    public ConcurrentSkipListSet<Long> getTransactionKeyList() { return transactionKeyList; }

    public void setTransactionKeyList(ConcurrentSkipListSet<Long> transactionKeyList) { this.transactionKeyList = transactionKeyList; }

    public PartitionIndexVo getIndexVo() {
        return indexVo;
    }

    public DataSyncInfo getSyncInfo() {
        return syncInfo;
    }

    public void setSyncInfo(DataSyncInfo syncInfo) {
        this.syncInfo = syncInfo;
    }

    public long getTransactionKey() {
        return indexVo.getTransactionIndex();
    }

    public long getGroupKey() {
        return indexVo.getGroupIndex();
    }
}
