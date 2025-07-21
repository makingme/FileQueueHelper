package com.queue.file.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.queue.file.exception.*;
import com.queue.file.vo.*;
import org.h2.mvstore.MVMap;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * @since : 2025-07-16(수)
 */
public class DataAccess {
    private final StatsTracker statsTracker = new StatsTracker();
    private final StoreInfo storeInfo;
    private final PartitionManager partitionManager;

    public DataAccess(StoreInfo storeInfo, PartitionManager partitionManager) {
        this.storeInfo = storeInfo;
        this.partitionManager = partitionManager;
    }

    public void write(String tag, String partition, String executorName, String data) throws QueueException {
        if (data == null || data.isEmpty()) {
            return;
        }
        writeQueueData(executorName, Collections.singletonList(new FileQueueData(partition, tag, data)));
    }

    public void writeBulk(String tag, String partition, String executorName, List<String> dataList) throws QueueException {
        if (dataList ==null || dataList.isEmpty()) {
            return;
        }
        List<FileQueueData> storeDataList = new ArrayList<>(dataList.size());
        for (String data : dataList) {
            storeDataList.add(new FileQueueData(partition, tag, data));
        }
        writeQueueData(executorName, storeDataList);
    }

    public void writeQueueData(String executorName, List<FileQueueData> storeDataList) throws QueueException {
        // store 상태 체크
        try{
            partitionManager.checkStore();
        }catch (UnsteadyStateException e) {
            throw new QueueException(e);
        }
        
        if (storeDataList==null || storeDataList.isEmpty()) {
            return;
        }

        FileQueueConfigVo configVo = storeInfo.getCONFIG();
        FileQueueCustomConfigVo customConfigVo = configVo.getCustomConfig();
        Map<String, List<FileQueueData>> groupedByPartition = storeDataList.stream()
                .collect(Collectors.groupingBy(FileQueueData::getPartition));

        // 입력 파티션에 일제 쓰기락 획득
        List<String> acquiredPartitions = acquireAllPartitionLocks(groupedByPartition.keySet());
        boolean needsRollback = false;
        try {
            for (Map.Entry<String, List<FileQueueData>> entry : groupedByPartition.entrySet()) {
                String partitionName = entry.getKey();
                List<FileQueueData> partitionDataList = entry.getValue();
                processPartitionData(partitionName, partitionDataList, executorName);
            }

            int totalDataCount = storeDataList.size();
            boolean isBulk = totalDataCount >= customConfigVo.getBulkSize();
            if (isBulk && customConfigVo.isBulkCommit()) {
                storeInfo.getStore().commit();
                needsRollback = true;
            }

        }catch (Exception e) {
            if (needsRollback) {
                try {
                    storeInfo.getStore().rollback();
                }catch (Exception e1) {
                    e.addSuppressed(e1);
                }
            }
            throw new QueueWriteException("큐 입력 중 예외 발생 = 큐:[" + configVo.getQueueName() + "]", e);
        }finally {
            // 모든 락 해제 (역순)
            releaseAllLocks(acquiredPartitions);
        }
    }

    private List<String> acquireAllPartitionLocks(Set<String> partitionNames) throws QueueException {
        List<String> sortedPartitions = new ArrayList<>(partitionNames);
        // 일관된 락 순서
        Collections.sort(sortedPartitions);
        try {
            for (String partitionName : sortedPartitions) {
                PartitionContext partitionContext;
                try {
                    partitionContext = partitionManager.getPartitionContext(partitionName);
                } catch (InitializeException e) {
                    throw new QueueException("신규 파티션 입력 - 파일 쓰기 중 "+partitionName + " 파티션 Context 생성 중 예외 발생", e);
                }

                ReentrantReadWriteLock lock = partitionContext.getLock();
                lock.writeLock().lock();
            }
            return sortedPartitions;

        } catch (Exception e) {
            // 획득한 락들을 역순으로 해제
            releaseAllLocks(sortedPartitions);
            throw new QueueException(e);
        }
    }

    private void processPartitionData(String partitionName, List<FileQueueData> partitionDataList, String executorName) throws QueueException {
        PartitionContext partitionContext = null;
        try{
            partitionContext = partitionManager.getPartitionContext(partitionName);
        }catch (InitializeException e){
            throw new QueueWriteException(partitionName + " 신규 파티션 Context 생성 중 예외 발생", e);
        }
        try {
            long groupKey = partitionContext.getGroupKey();
            ConcurrentSkipListSet<Long> dataKeyList = partitionContext.getTransactionKeyList();
            MVMap<Long, FileQueueData> dataMap = partitionContext.getDataMap();
            if (dataMap == null || dataMap.isClosed()) {
                throw new QueueWriteException(partitionContext.getPartitionName() + " 파티션 데이터 객체가 비정상적인 상태");
            }
            for (FileQueueData queueData : partitionDataList) {
                long innerKey = partitionContext.getTransactionKey();
                queueData.setTransactionKey(innerKey);
                queueData.setGroupTransactionKey(groupKey);
                dataMap.put(innerKey, queueData);
                dataKeyList.add(innerKey);
            }
            statsTracker.keepRecord(partitionName, executorName, partitionDataList.size(), ActionType.INPUT);
        }catch (Exception e){
            throw new QueueWriteException(
                    "파티션 데이터 처리 중 예외 발생 - 파티션: " + partitionContext.getPartitionName(), e);
        }

    }

    private void releaseAllLocks(List<String> acquiredLockPartitions) {
        // 역순으로 락 해제
        for (int i = acquiredLockPartitions.size() - 1; i >= 0; i--) {
            PartitionContext partitionContext;
            try {
                partitionContext = partitionManager.getPartitionContext(acquiredLockPartitions.get(i));
            } catch (InitializeException e) {
                continue;
            }
            try {
                partitionContext.getLock().writeLock().unlock();
            } catch (Exception e) {
                System.err.println("락 해제 중 예외 발생: " + e.getMessage());
            }
        }
    }

    public FileQueueData read(String partitionName, String executorName) throws QueueException {
        List<FileQueueData> fileQueueDataList = read(partitionName, executorName, 1);
        return (fileQueueDataList == null || fileQueueDataList.isEmpty()) ? null : fileQueueDataList.get(0);
    }

    public List<FileQueueData> read(String partitionName, String executorName, int requestCount) throws QueueException {
        if (storeInfo.getStoreOpenTime() == null) {
            throw new QueueReadException("open 되지 않음 - open() 호출 필요");
        }
        FileQueueConfigVo configVo = storeInfo.getCONFIG();
        FileQueueCustomConfigVo customConfigVo = configVo.getCustomConfig();
        statsTracker.keepRecord(partitionName, executorName, ActionType.OUTPUT_INVOKE);
        boolean isCommited = false;
        if (customConfigVo.isStableMode()) {
            List<FileQueueData> bufferDataList = readBuffer(partitionName, executorName);
            if (bufferDataList != null && !bufferDataList.isEmpty()) {
                return bufferDataList;
            }
        }
        PartitionContext partitionContext = partitionManager.getPartitionContextMap().get(partitionName);
        if (partitionContext == null) {
            return null;
        }
        ReentrantReadWriteLock lock = partitionContext.getLock();
        ConcurrentSkipListSet<Long> dataKeyList = partitionContext.getTransactionKeyList();
        MVMap<Long, FileQueueData> dataMap = partitionContext.getDataMap();
        if (dataMap == null || dataMap.isClosed()) {
            throw new QueueReadException(partitionName + "파티션 데이터 영역이 비정상적인 상태");
        }

        lock.readLock().lock();
        try{
            if (dataMap.isEmpty()) {
                return null;
            }
        }finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            int selectCount = Math.min(requestCount, dataMap.size());
            if(selectCount < 1){
                return null;
            }
            boolean isBulk = selectCount >= customConfigVo.getBulkSize();
            List<FileQueueData> queueDataList = extractData(partitionName, dataKeyList, dataMap, selectCount);
            if (customConfigVo.isStableMode()) {
                MVMap<String, List<FileQueueData>> readBufferMap = partitionContext.getReadBufferMap();
                if (readBufferMap != null && !readBufferMap.isClosed()) {
                    readBufferMap.put(executorName, queueDataList);
                    statsTracker.keepRecord(partitionName, executorName, ActionType.BUFFER_INPUT);
                }
            }
            if (isBulk && customConfigVo.isBulkCommit()) {
                storeInfo.getStore().commit();
                isCommited = true;
            }
            statsTracker.keepRecord(partitionName, executorName, queueDataList.size(), ActionType.OUTPUT);
            return queueDataList;
        } catch (QueueReadException e) {
            throw new QueueReadException("큐:[" + configVo.getQueueName() + "] " + e.getMessage(), e);
        } catch (Exception e) {
            if (isCommited) {
                storeInfo.getStore().rollback();
            }
            throw new QueueReadException("<읽기 : 실패> = 큐:[" + configVo.getQueueName() + "]", e);
        }finally {
            lock.writeLock().unlock();
        }
    }

    private List<FileQueueData> extractData(String partitionName, ConcurrentSkipListSet<Long> dataKeyList, MVMap<Long, FileQueueData> dataMap, int selectCount) throws QueueException {
        List<FileQueueData> queueDataList = new ArrayList<>(selectCount);
        try {

            partitionManager.realignDataKey(partitionName);
            for (int i = 1; i <= selectCount; i++) {
                Long transKey = dataKeyList.pollFirst();
                if (transKey == null) {
                    continue;
                }
                FileQueueData data = dataMap.remove(transKey);
                if (data == null) {
                    continue;
                }
                queueDataList.add(data);
            }
            return queueDataList;
        } catch (Exception e) {
            throw new QueueReadException("데이터 추출 중 예외 발생 = 파티션:" + partitionName, e);
        }
    }

    private List<FileQueueData> readBuffer(String partitionName, String executorName) throws QueueException {
        statsTracker.keepRecord(partitionName, executorName, ActionType.BUFFER_OUTPUT_INVOKE);
        List<FileQueueData> queueDataList = null;
        PartitionContext partitionContext = partitionManager.getPartitionContextMap().get(partitionName);
        if (partitionContext == null) {
            return null;
        }
        MVMap<String, List<FileQueueData>> readBufferMap = partitionContext.getReadBufferMap();
        if (readBufferMap == null || readBufferMap.isClosed()) {
           return null;
        }
        ReentrantReadWriteLock lock = partitionContext.getLock();
        lock.readLock().lock();
        String jsonData = null;
        try {
            queueDataList = readBufferMap.get(executorName);
            return queueDataList;
        } catch (Exception e) {
            throw new QueueReadException(partitionName + "파티션의 " + executorName + "버퍼 영역 데이터 파싱 중 예외 발생 - 원본 데이터 정보:" + queueDataList, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void readCommit(String partitionName, String executorName) throws QueueException {
        if (storeInfo.getStoreOpenTime() == null) {
            throw new QueueReadException("open 되지 않음 - open() 호출 필요");
        }
        PartitionContext partitionContext = partitionManager.getPartitionContextMap().get(partitionName);
        if (partitionContext == null) {
            return;
        }
        MVMap<String, List<FileQueueData>> readBufferMap = partitionContext.getReadBufferMap();
        if (readBufferMap == null || readBufferMap.isClosed()) {
            return;
        }

        ReentrantReadWriteLock lock = partitionContext.getLock();
        lock.writeLock().lock();
        try {
            readBufferMap.remove(executorName);
            statsTracker.keepRecord(partitionName, executorName, ActionType.BUFFER_OUTPUT);
        } catch (Exception e) {
            throw new QueueReadException("[" + storeInfo.getCONFIG().getQueueName() + "] 큐, 버퍼 COMMIT 실패 - 파티션:" + partitionName, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

}
