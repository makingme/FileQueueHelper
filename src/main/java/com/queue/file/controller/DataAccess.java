package com.queue.file.controller;

import com.queue.file.exception.*;
import com.queue.file.vo.*;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @since : 2025-07-16(수)
 */
public class DataAccess {
    private static final Logger logger = LoggerFactory.getLogger(DataAccess.class);
    private final StatsTracker statsTracker = new StatsTracker();
    private final StoreInfo storeInfo;
    private final QueueManager queueManager;

    public DataAccess(StoreInfo storeInfo, QueueManager queueManager) {
        this.storeInfo = storeInfo;
        this.queueManager = queueManager;
    }

    public void write(String tag, String executorName, String data) throws QueueException {
        if (data == null || data.isEmpty()) {
            return;
        }
        logger.debug("write tag={} executor={}", tag, executorName);
        writeQueueData(executorName, Collections.singletonList(new FileQueueData(tag, data)));
    }

    public void writeBulk(String tag, String executorName, List<String> dataList) throws QueueException {
        if (dataList ==null || dataList.isEmpty()) {
            return;
        }
        logger.debug("writeBulk tag={} executor={} size={}", tag, executorName, dataList.size());
        List<FileQueueData> storeDataList = new ArrayList<>(dataList.size());
        for (String data : dataList) {
            storeDataList.add(new FileQueueData(tag, data));
        }
        writeQueueData(executorName, storeDataList);
    }

    public void writeQueueData(String executorName, List<FileQueueData> storeDataList) throws QueueException {
        // store 상태 체크
        try{
            queueManager.checkStore();
        }catch (UnsteadyStateException e) {
            throw new QueueException(e);
        }

        logger.debug("writeQueueData executor={} size={}", executorName, storeDataList==null?0:storeDataList.size());

        if (storeDataList==null || storeDataList.isEmpty()) {
            return;
        }

        FileQueueConfigVo configVo = storeInfo.getCONFIG();
        FileQueueCustomConfigVo customConfigVo = configVo.getCustomConfig();

        QueueContext ctx = queueManager.getQueueContext();
        ReentrantReadWriteLock.WriteLock writeLock = ctx.getLock().writeLock();

        writeLock.lock();
        boolean needsRollback = false;
        try {
            long groupKey = ctx.getIndexVo().getGroupIndex();
            ConcurrentSkipListSet<Long> dataKeyList = ctx.getTransactionKeyList();
            MVMap<Long, FileQueueData> dataMap = ctx.getDataMap();

            if (dataMap == null || dataMap.isClosed()) {
                throw new QueueWriteException("데이터 객체가 비정상적인 상태");
            }

            for (FileQueueData queueData : storeDataList) {
                long innerKey = ctx.getIndexVo().getTransactionIndex();
                queueData.setTransactionKey(innerKey);
                queueData.setGroupTransactionKey(groupKey);
                dataMap.put(innerKey, queueData);
                dataKeyList.add(innerKey);
            }

            int totalDataCount = storeDataList.size();
            boolean isBulk = totalDataCount >= customConfigVo.getBulkSize();

            if (isBulk && customConfigVo.isBulkCommit()) {
                storeInfo.getStore().commit();
                needsRollback = true;
            }

            statsTracker.keepRecord(executorName, storeDataList.size(), ActionType.INPUT);

        }catch (Exception e) {
            if (needsRollback) {
                try {
                    storeInfo.getStore().rollback();
                }catch (Exception e1) {
                    e.addSuppressed(e1);
                }
            }
            logger.error("writeQueueData failed", e);
            throw new QueueWriteException("큐 입력 중 예외 발생 = 큐:[" + configVo.getQueueName() + "]", e);
        }finally {
            writeLock.unlock();
        }
    }

    public FileQueueData read(String executorName) throws QueueException {
        logger.debug("read single executor={}", executorName);
        List<FileQueueData> fileQueueDataList = read(executorName, 1);
        return (fileQueueDataList == null || fileQueueDataList.isEmpty()) ? null : fileQueueDataList.get(0);
    }

    public List<FileQueueData> read(String executorName, int requestCount) throws QueueException {
        logger.debug("read executor={} count={}", executorName, requestCount);
        if (storeInfo.getStoreOpenTime() == null) {
            throw new QueueReadException("open 되지 않음 - open() 호출 필요");
        }
        FileQueueConfigVo configVo = storeInfo.getCONFIG();
        FileQueueCustomConfigVo customConfigVo = configVo.getCustomConfig();
        statsTracker.keepRecord(executorName, ActionType.OUTPUT_INVOKE);

        // Stable 모드: 버퍼 확인
        if (customConfigVo.isStableMode()) {
            List<FileQueueData> bufferDataList = readBuffer(executorName);
            if (bufferDataList != null && !bufferDataList.isEmpty()) {
                return bufferDataList;
            }
        }

        QueueContext ctx = queueManager.getQueueContext();
        MVMap<Long, FileQueueData> dataMap = ctx.getDataMap();
        if (dataMap == null || dataMap.isClosed()) {
            throw new QueueReadException("데이터 영역이 비정상적인 상태");
        }

        ReentrantReadWriteLock.WriteLock writeLock = ctx.getLock().writeLock();
        writeLock.lock();
        boolean isCommitted = false;
        try {
            int selectCount = Math.min(requestCount, dataMap.size());
            if(selectCount < 1){
                return null;
            }

            boolean isBulk = selectCount >= customConfigVo.getBulkSize();
            List<FileQueueData> queueDataList = extractData(ctx, selectCount);

            if (customConfigVo.isStableMode()) {
                MVMap<String, List<FileQueueData>> readBufferMap = ctx.getReadBufferMap();
                if (readBufferMap != null && !readBufferMap.isClosed()) {
                    readBufferMap.put(executorName, queueDataList);
                    statsTracker.keepRecord(executorName, ActionType.BUFFER_INPUT);
                }
            }

            if (isBulk && customConfigVo.isBulkCommit()) {
                storeInfo.getStore().commit();
                isCommitted = true;
            }

            statsTracker.keepRecord(executorName, queueDataList.size(), ActionType.OUTPUT);
            return queueDataList;

        } catch (QueueReadException e) {
            logger.error("read failed: {}", e.getMessage());
            throw new QueueReadException("큐:[" + configVo.getQueueName() + "] " + e.getMessage(), e);
        } catch (Exception e) {
            if (isCommitted) {
                try {
                    storeInfo.getStore().rollback();
                } catch (Exception e1) {
                    e.addSuppressed(e1);
                }
            }
            logger.error("read failed", e);
            throw new QueueReadException("<읽기 : 실패> = 큐:[" + configVo.getQueueName() + "]", e);
        }finally {
            writeLock.unlock();
        }
    }

    private List<FileQueueData> extractData(QueueContext ctx, int selectCount) throws QueueException {
        logger.debug("extractData count={}", selectCount);
        List<FileQueueData> queueDataList = new ArrayList<>(selectCount);
        try {
            queueManager.realignDataKey();

            ConcurrentSkipListSet<Long> dataKeyList = ctx.getTransactionKeyList();
            MVMap<Long, FileQueueData> dataMap = ctx.getDataMap();

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
            logger.error("extractData failed", e);
            throw new QueueReadException("데이터 추출 중 예외 발생", e);
        }
    }

    private List<FileQueueData> readBuffer(String executorName) throws QueueException {
        statsTracker.keepRecord(executorName, ActionType.BUFFER_OUTPUT_INVOKE);
        logger.debug("readBuffer executor={}", executorName);

        QueueContext ctx = queueManager.getQueueContext();
        MVMap<String, List<FileQueueData>> readBufferMap = ctx.getReadBufferMap();
        if (readBufferMap == null || readBufferMap.isClosed()) {
           return null;
        }

        ReentrantReadWriteLock.ReadLock readLock = ctx.getLock().readLock();
        readLock.lock();
        try {
            List<FileQueueData> queueDataList = readBufferMap.get(executorName);
            return queueDataList;
        } catch (Exception e) {
            logger.error("readBuffer failed executor={}", executorName, e);
            throw new QueueReadException(executorName + "버퍼 영역 데이터 파싱 중 예외 발생", e);
        } finally {
            readLock.unlock();
        }
    }

    public void readCommit(String executorName) throws QueueException {
        if (storeInfo.getStoreOpenTime() == null) {
            throw new QueueReadException("open 되지 않음 - open() 호출 필요");
        }

        QueueContext ctx = queueManager.getQueueContext();
        MVMap<String, List<FileQueueData>> readBufferMap = ctx.getReadBufferMap();
        if (readBufferMap == null || readBufferMap.isClosed()) {
            return;
        }

        ReentrantReadWriteLock.WriteLock writeLock = ctx.getLock().writeLock();
        writeLock.lock();
        try {
            logger.debug("readCommit executor={}", executorName);
            readBufferMap.remove(executorName);
            statsTracker.keepRecord(executorName, ActionType.BUFFER_OUTPUT);
        } catch (Exception e) {
            logger.error("readCommit failed", e);
            throw new QueueReadException("[" + storeInfo.getCONFIG().getQueueName() + "] 큐, 버퍼 COMMIT 실패", e);
        } finally {
            writeLock.unlock();
        }
    }

    /* *****************************************************************
     *  Management helper methods used by BaseController
     * *****************************************************************/

    /** 요약 정보 가져오는 함수 - 영역 별 데이터 갯수 */
    public QueueSummaryVo getSummaryInfo() {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().readLock().lock();
        try {
            int dataCnt = ctx.getDataMap() == null ? 0 : ctx.getDataMap().size();
            int bufferCnt = ctx.getReadBufferMap() == null ? 0 : ctx.getReadBufferMap().size();
            int cacheCnt = ctx.getCacheMap() == null ? 0 : ctx.getCacheMap().size();
            return new QueueSummaryVo(dataCnt, bufferCnt, cacheCnt);
        } finally {
            ctx.getLock().readLock().unlock();
        }
    }

    /** 데이터 목록 가져 오는 함수 */
    public List<FileQueueData> getDataList() {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().readLock().lock();
        try {
            if (ctx.getDataMap() == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(ctx.getDataMap().values());
        } finally {
            ctx.getLock().readLock().unlock();
        }
    }

    /** 버퍼 목록 가져 오는 함수 */
    public Map<String, List<FileQueueData>> getBufferList() {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().readLock().lock();
        try {
            if (ctx.getReadBufferMap() == null) {
                return Collections.emptyMap();
            }
            return new HashMap<>(ctx.getReadBufferMap());
        } finally {
            ctx.getLock().readLock().unlock();
        }
    }

    /** 캐시 목록 가져 오는 함수 */
    public Map<String, Object> getCacheList() {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().readLock().lock();
        try {
            if (ctx.getCacheMap() == null) {
                return Collections.emptyMap();
            }
            return new HashMap<>(ctx.getCacheMap());
        } finally {
            ctx.getLock().readLock().unlock();
        }
    }

    /** 특정 데이터 정보 가져 오는 함수 */
    public FileQueueData getData(Long transactionKey) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().readLock().lock();
        try {
            return ctx.getDataMap() == null ? null : ctx.getDataMap().get(transactionKey);
        } finally {
            ctx.getLock().readLock().unlock();
        }
    }

    /** 전체 데이터 정보 삭제 함수 */
    public void clearData() {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().writeLock().lock();
        try {
            if (ctx.getDataMap() != null) {
                ctx.getDataMap().clear();
            }
            if (ctx.getTransactionKeyList() != null) {
                ctx.getTransactionKeyList().clear();
            }
        } finally {
            ctx.getLock().writeLock().unlock();
        }
    }

    /** 특정 데이터 정보 삭제 함수 */
    public void removeData(Long transactionKey) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().writeLock().lock();
        try {
            if (ctx.getDataMap() != null) {
                ctx.getDataMap().remove(transactionKey);
            }
            if (ctx.getTransactionKeyList() != null) {
                ctx.getTransactionKeyList().remove(transactionKey);
            }
        } finally {
            ctx.getLock().writeLock().unlock();
        }
    }

    /** 특정 버퍼 정보 가져오는 함수 */
    public List<FileQueueData> getBuffer(String executorName) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().readLock().lock();
        try {
            Map<String, List<FileQueueData>> map = ctx.getReadBufferMap();
            if (map == null) {
                return Collections.emptyList();
            }
            List<FileQueueData> list = map.get(executorName);
            return list == null ? Collections.emptyList() : new ArrayList<>(list);
        } finally {
            ctx.getLock().readLock().unlock();
        }
    }

    /** 모든 버퍼 정보 삭제 함수 */
    public void clearBuffer() {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().writeLock().lock();
        try {
            if (ctx.getReadBufferMap() != null) {
                ctx.getReadBufferMap().clear();
            }
        } finally {
            ctx.getLock().writeLock().unlock();
        }
    }

    /** 특정 버퍼 정보 삭제 함수 */
    public void clearBuffer(String executorName) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().writeLock().lock();
        try {
            if (ctx.getReadBufferMap() != null) {
                ctx.getReadBufferMap().remove(executorName);
            }
        } finally {
            ctx.getLock().writeLock().unlock();
        }
    }

    /** 특정 캐시 정보 가져오는 함수 */
    public Object getCache(String cacheKey) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().readLock().lock();
        try {
            return ctx.getCacheMap() == null ? null : ctx.getCacheMap().get(cacheKey);
        } finally {
            ctx.getLock().readLock().unlock();
        }
    }

    /** 모든 캐시 정보 삭제 함수 */
    public void clearCache() {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().writeLock().lock();
        try {
            if (ctx.getCacheMap() != null) {
                ctx.getCacheMap().clear();
            }
        } finally {
            ctx.getLock().writeLock().unlock();
        }
    }

    /** 특정 캐시 정보 삭제 함수 */
    public void clearCache(String cacheKey) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().writeLock().lock();
        try {
            if (ctx.getCacheMap() != null) {
                ctx.getCacheMap().remove(cacheKey);
            }
        } finally {
            ctx.getLock().writeLock().unlock();
        }
    }

    /** 버퍼 또는 데이터 한 건 제거 */
    public void removeOne(String executorName) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().writeLock().lock();
        try {
            MVMap<String, List<FileQueueData>> bufferMap = ctx.getReadBufferMap();
            if (bufferMap != null && bufferMap.containsKey(executorName)) {
                bufferMap.remove(executorName);
                return;
            }

            MVMap<Long, FileQueueData> dataMap = ctx.getDataMap();
            if (dataMap == null || dataMap.isEmpty()) {
                return;
            }

            ConcurrentSkipListSet<Long> keyList = ctx.getTransactionKeyList();
            if (keyList != null) {
                try {
                    queueManager.realignDataKey();
                } catch (UnsteadyStateException ignore) {
                    // ignore and continue
                }
                Long firstKey = keyList.pollFirst();
                if (firstKey != null) {
                    dataMap.remove(firstKey);
                    keyList.remove(firstKey);
                }
            }
        } finally {
            ctx.getLock().writeLock().unlock();
        }
    }
}
