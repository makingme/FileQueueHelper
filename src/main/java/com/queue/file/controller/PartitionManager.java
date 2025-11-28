package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import com.queue.file.exception.UnsteadyStateException;
import com.queue.file.utils.Contents;
import com.queue.file.vo.*;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * v2.1.0: Simplified PartitionManager (single queue only, no partition concept)
 * @since : 2025-11-28
 */
public class PartitionManager {
    private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);
    private final StoreInfo storeInfo;
    private final QueueContext queueContext;

    public PartitionManager(StoreInfo storeInfo) {
        this.storeInfo = storeInfo;
        logger.debug("initialize PartitionManager (v2.1.0 - single queue mode)");
        this.queueContext = initializeQueue();
    }

    private QueueContext initializeQueue() throws InitializeException {
        logger.debug("initializeQueue");
        MVStore store = storeInfo.getStore();
        if (store == null || store.isClosed()) {
            throw new InitializeException("스토어가 초기화되지 않음 또는 닫힘");
        }

        QueueContext context = new QueueContext();
        try {
            context.setDataMap(store.openMap(Contents.getDataMapName()));
            context.setReadBufferMap(store.openMap(Contents.getReadBufferName()));
            context.setCacheMap(store.openMap(Contents.getCacheName()));
        } catch (Exception e) {
            throw new InitializeException("큐 데이터 오픈 중 예외 발생: " + e.getMessage());
        }

        realignDataKey(context);
        return context;
    }

    private void realignDataKey(QueueContext context) throws UnsteadyStateException {
        logger.debug("realignDataKey");
        var dataKeyList = context.getTransactionKeyList();
        MVMap<Long, FileQueueData> dataMap = context.getDataMap();

        if(dataMap == null) {
            throw new UnsteadyStateException("데이터 영역이 존재하지 않음");
        }

        int dataCount = dataMap.size();
        long dataMapMaxKey = dataCount == 0 ? 0L : Collections.max(dataMap.keySet());
        int keyCount = dataKeyList.size();
        long keyListMaxKey = keyCount == 0 ? 0L : dataKeyList.last();

        if (dataCount != keyCount || (dataMapMaxKey != keyListMaxKey)) {
            dataKeyList.clear();
            dataKeyList.addAll(dataMap.keySet());
            DataSyncInfo dataSyncInfo = new DataSyncInfo(dataCount, keyCount, dataCount - keyCount, Math.max(dataMapMaxKey, keyListMaxKey));
            PartitionIndexVo indexVo = context.getIndexVo();
            indexVo.setTransactionIndex(dataSyncInfo.getMaxKey() + 1);
            context.setSyncInfo(dataSyncInfo);
        }
    }

    public void checkState() throws UnsteadyStateException {
        logger.debug("checkState");
        checkStore();
        MVMap<Long, FileQueueData> dataMap = queueContext.getDataMap();
        if (dataMap == null || dataMap.isClosed()) {
            throw new UnsteadyStateException("데이터 영역이 유효하지 않은 상태");
        }
        MVMap<String, java.util.List<FileQueueData>> readBufferMap = queueContext.getReadBufferMap();
        if (readBufferMap == null || readBufferMap.isClosed()) {
            throw new UnsteadyStateException("Read Buffer 영역이 유효하지 않은 상태");
        }
    }

    public void checkStore() throws UnsteadyStateException {
        logger.debug("checkStore");
        MVStore store = storeInfo.getStore();
        if (store == null || store.isClosed()) {
            throw new UnsteadyStateException(storeInfo.getCONFIG().getQueueName() + " 파일큐가 유효하지 않은 상태");
        }
        if (!Files.exists(Paths.get(storeInfo.getCONFIG().getQueue()))) {
            throw new UnsteadyStateException(storeInfo.getCONFIG().getQueue() + " 파일큐를 찾을 수 없음 - 삭제됨");
        }
    }

    /**
     * v2.1.0: Always returns the single queue context (partition parameter ignored for compatibility)
     */
    public QueueContext getPartitionContext(String partitionName) throws InitializeException{
        return queueContext;
    }

    public QueueContext getQueueContext() {
        return queueContext;
    }
}
