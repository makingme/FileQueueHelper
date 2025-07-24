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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @since : 2025-07-16(수)
 */
public class PartitionManager {
    private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);
    private final StoreInfo storeInfo;
    private final Map<String, PartitionContext> partitionContextMap = new ConcurrentHashMap<>();

    public PartitionManager(StoreInfo storeInfo) {
        this.storeInfo = storeInfo;
        logger.debug("initialize PartitionManager");
        initializePartitions();
    }

    private void initializePartitions() throws InitializeException {
        logger.debug("initializePartitions");
        MVStore store = storeInfo.getStore();
        if (store == null || store.isClosed()) {
            throw new InitializeException("스토어가 초기화되지 않음 또는 닫힘");
        }
        Set<String> schemaNameSet = store.getMapNames();
        if (schemaNameSet == null || schemaNameSet.isEmpty()) {
            try {
                PartitionContext partitionContext = new PartitionContext(Contents.DEFAULT_PARTITION);
                partitionContext.setDataMap(store.openMap(Contents.getDataMapName()));
                partitionContext.setReadBufferMap(store.openMap(Contents.getReadBufferName()));
                partitionContext.setCacheMap(store.openMap(Contents.getCacheName()));
                partitionContextMap.put(Contents.DEFAULT_PARTITION, partitionContext);
            } catch (Exception e) {
                throw new InitializeException("기본 파티션 데이터 오픈 중 예외 발생: " + e.getMessage());
            }
        } else {
            for (String schemaName : schemaNameSet) {
                int idx = schemaName.indexOf(Contents.DELIMITER);
                if (idx <= 0) continue;
                String partitionName = schemaName.substring(0, idx);
                String suffix = schemaName.substring(idx + 1);
                PartitionContext partitionContext = partitionContextMap.computeIfAbsent(partitionName, PartitionContext::new);
                try {
                    switch (suffix) {
                        case Contents.DATA_MAP_SUFFIX:
                            partitionContext.setDataMap(store.openMap(Contents.getDataMapName(partitionName)));
                            break;
                        case Contents.READ_BUFFER_SUFFIX:
                            partitionContext.setReadBufferMap(store.openMap(Contents.getReadBufferName(partitionName)));
                            break;
                        case Contents.CACHE_SUFFIX:
                            partitionContext.setCacheMap(store.openMap(Contents.getCacheName(partitionName)));
                            break;
                    }
                } catch (Exception e) {
                    throw new InitializeException("내부 스키마[" + schemaName + "] 데이터 오픈 중 예외 발생: " + e.getMessage());
                }
            }
            realignAllPartitions();
        }
    }

    private void realignAllPartitions() throws InitializeException {
        logger.debug("realignAllPartitions");
        MVStore store = storeInfo.getStore();
        if (store == null || store.isClosed()) {
            throw new InitializeException("스토어가 초기화되지 않음 또는 닫힘");
        }

        for (PartitionContext partitionContext : partitionContextMap.values()) {
            partitionContext.getLock().writeLock().lock();
            String partitionName = partitionContext.getPartitionName();
            try {
                realignDataKey(partitionName);
            } catch (UnsteadyStateException e) {
                throw new InitializeException("파티션 [" + partitionName + "] 재정렬 중 예외 발생", e);
            }finally {
                partitionContext.getLock().writeLock().unlock();
            }
        }
    }

    public void realignDataKey(String partitionName) throws UnsteadyStateException {
        logger.debug("realignDataKey partition={}", partitionName);
        PartitionContext partitionContext = partitionContextMap.get(partitionName);
        if (partitionContext == null) {
            throw new UnsteadyStateException(partitionName + " 파티션이 존재하지 않음");
        }
        ConcurrentSkipListSet<Long> dataKeyList = partitionContext.getTransactionKeyList();
        MVMap<Long, FileQueueData> dataMap = partitionContext.getDataMap();
        if(dataMap == null) {
            throw new UnsteadyStateException(partitionName + " 파티션에 데이터 영역이 존재하지 않음");
        }
        int dataCount = dataMap.size();
        long dataMapMaxKey = dataCount == 0 ? 0L : Collections.max(dataMap.keySet());
        int keyCount = dataKeyList.size();
        long keyListMaxKey = keyCount == 0 ? 0L : dataKeyList.last();

        if (dataCount != keyCount || (dataMapMaxKey != keyListMaxKey)) {
            dataKeyList.clear();
            dataKeyList.addAll(dataMap.keySet());
            DataSyncInfo dataSyncInfo = new DataSyncInfo(dataCount, keyCount, dataCount - keyCount, Math.max(dataMapMaxKey, keyListMaxKey));
            PartitionIndexVo indexVo = partitionContext.getIndexVo();
            indexVo.setTransactionIndex(dataSyncInfo.getMaxKey() + 1);
            partitionContext.setSyncInfo(dataSyncInfo);
        }
    }

    public void checkAllState() throws UnsteadyStateException {
        logger.debug("checkAllState");
        checkStore();
        for (String partitionName : partitionContextMap.keySet()) {
            checkState(partitionName, false);
        }
    }

    public void checkState(String partitionName, boolean checkStore) throws UnsteadyStateException {
        logger.debug("checkState partition={}", partitionName);
        if (checkStore) {
            checkStore();
        }
        PartitionContext partitionContext = partitionContextMap.get(partitionName);
        if (partitionContext == null) {
            throw new UnsteadyStateException(partitionName + " 해당 파티션을 찾을 수 없음");
        }
        MVMap<Long, FileQueueData> dataMap = partitionContext.getDataMap();
        if (dataMap == null || dataMap.isClosed()) {
            throw new UnsteadyStateException(partitionName + " 파티션의 데이터 영역이 유효하지 않은 상태");
        }
        MVMap<String, List<FileQueueData>> readBufferMap = partitionContext.getReadBufferMap();
        if (readBufferMap == null || readBufferMap.isClosed()) {
            throw new UnsteadyStateException(partitionName + " 파티션의 Read Buffer 영역이 유효하지 않은 상태");
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

    public PartitionContext getPartitionContext(String partitionName) throws InitializeException{
        return partitionContextMap.computeIfAbsent(partitionName, k -> {
            MVStore store = storeInfo.getStore();
            PartitionContext context = new PartitionContext(k);
            context.setDataMap(store.openMap(Contents.getDataMapName(k)));
            context.setReadBufferMap(store.openMap(Contents.getReadBufferName(k)));
            context.setCacheMap(store.openMap(Contents.getCacheName(k)));
         return context;
        });
    }

    public Map<String, PartitionContext> getPartitionContextMap() {
        return partitionContextMap;
    }
}
