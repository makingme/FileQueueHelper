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
 * v3.0.0: Manages single queue context (partition concept completely removed)
 * @since : 2025-11-28
 */
public class QueueManager {
    private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);
    private final StoreInfo storeInfo;
    private final QueueContext queueContext;

    public QueueManager(StoreInfo storeInfo) {
        this.storeInfo = storeInfo;
        logger.debug("initialize QueueManager (v3.0.0 - single queue only)");
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

        realignDataKey();
        return context;
    }

    public void realignDataKey() throws UnsteadyStateException {
        logger.debug("realignDataKey");
        var dataKeyList = queueContext.getTransactionKeyList();
        MVMap<Long, FileQueueData> dataMap = queueContext.getDataMap();

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
            PartitionIndexVo indexVo = queueContext.getIndexVo();
            indexVo.setTransactionIndex(dataSyncInfo.getMaxKey() + 1);
            queueContext.setSyncInfo(dataSyncInfo);
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

    public QueueContext getQueueContext() {
        return queueContext;
    }
}
