package com.queue.file.controller;

import com.queue.file.exception.QueueException;
import com.queue.file.exception.QueueReadException;
import com.queue.file.exception.QueueWriteException;
import com.queue.file.exception.UnsteadyStateException;
import com.queue.file.vo.*;
import org.h2.mvstore.MVMap;
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

    public void write(String tag, String producerId, String data) throws QueueException {
        if (data == null || data.isEmpty()) {
            return;
        }
        logger.debug("write tag={} executor={}", tag, producerId);
        writeQueueData(producerId, Collections.singletonList(new FileQueueData(tag, data)));
    }

    public void writeBulk(String tag, String producerId, List<String> dataList) throws QueueException {
        if (dataList ==null || dataList.isEmpty()) {
            return;
        }
        logger.debug("writeBulk tag={} executor={} size={}", tag, producerId, dataList.size());
        List<FileQueueData> storeDataList = new ArrayList<>(dataList.size());
        for (String data : dataList) {
            storeDataList.add(new FileQueueData(tag, data));
        }
        writeQueueData(producerId, storeDataList);
    }

    public void writeQueueData(String producerId, List<FileQueueData> storeDataList) throws QueueException {
        // store 상태 체크
        try{
            queueManager.checkStore();
        }catch (UnsteadyStateException e) {
            throw new QueueException(e);
        }

        logger.debug("writeQueueData executor={} size={}", producerId, storeDataList==null?0:storeDataList.size());

        if (storeDataList==null || storeDataList.isEmpty()) {
            return;
        }

        FileQueueConfigVo configVo = storeInfo.getCONFIG();
        FileQueueCustomConfigVo customConfigVo = configVo.getCustomConfig();

        QueueContext ctx = queueManager.getQueueContext();
        ReentrantReadWriteLock.WriteLock writeLock = ctx.getLock().writeLock();

        writeLock.lock();
        boolean isCommitted = false;  // v2.1.0: Renamed from needsRollback for clarity
        try {
            long groupKey = ctx.getIndexVo().getGroupIndex();
            ConcurrentSkipListSet<Long> dataKeyList = ctx.getTransactionKeyList();
            MVMap<Long, FileQueueData> dataMap = ctx.getDataMap();

            if (dataMap == null || dataMap.isClosed()) {
                throw new QueueWriteException(configVo.getQueueName(), "data map is closed or null");
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
                isCommitted = true;  // Mark as committed for potential rollback
            }

            statsTracker.keepRecord(producerId, storeDataList.size(), ActionType.INPUT);

        }catch (Exception e) {
            if (isCommitted) {  // Only rollback if we committed
                try {
                    storeInfo.getStore().rollback();
                }catch (Exception e1) {
                    e.addSuppressed(e1);
                }
            }
            logger.error("writeQueueData failed queue={} producer={} size={}", configVo.getQueueName(), producerId, storeDataList.size(), e);
            throw new QueueWriteException(configVo.getQueueName(), "writeQueueData failed", e);
        }finally {
            writeLock.unlock();
        }
    }

    public FileQueueData read(String consumerId) throws QueueException {
        logger.debug("read single executor={}", consumerId);
        List<FileQueueData> fileQueueDataList = read(consumerId, 1);
        return (fileQueueDataList == null || fileQueueDataList.isEmpty()) ? null : fileQueueDataList.get(0);
    }

    /**
     * Reads data from the queue and moves it to the read buffer.
     *
     * <p><b>Important:</b> This method uses WriteLock for data integrity.
     * It performs two operations atomically:
     * <ol>
     *   <li>Reads data from DataMap</li>
     *   <li><b>Removes data from DataMap and moves to ReadBuffer</b> (modification operation)</li>
     * </ol>
     *
     * <p>The write lock ensures data consistency during the move operation.
     * Concurrent reads are not supported by design to prioritize data integrity.
     * For concurrent scenarios, use separate executor names to isolate buffers.
     *
     * @param consumerId the executor name for buffer isolation
     * @param requestCount maximum number of items to read
     * @return list of queue data moved to the buffer
     * @throws QueueException if the queue is not opened or data map is in abnormal state
     * @since v2.1.0
     */
    public List<FileQueueData> read(String consumerId, int requestCount) throws QueueException {
        logger.debug("read executor={} count={}", consumerId, requestCount);
        FileQueueConfigVo configVo = storeInfo.getCONFIG();
        final String queueName = configVo.getQueueName();
        if (storeInfo.getStoreOpenTime() == null) {
            throw new QueueReadException(queueName, "store not opened; call open() first");
        }
        FileQueueCustomConfigVo customConfigVo = configVo.getCustomConfig();
        statsTracker.keepRecord(consumerId, ActionType.OUTPUT_INVOKE);

        // Stable 모드: 버퍼 확인
        if (customConfigVo.isStableMode()) {
            List<FileQueueData> bufferDataList = readBuffer(consumerId);
            if (bufferDataList != null && !bufferDataList.isEmpty()) {
                return bufferDataList;
            }
        }

        QueueContext ctx = queueManager.getQueueContext();
        MVMap<Long, FileQueueData> dataMap = ctx.getDataMap();
        if (dataMap == null || dataMap.isClosed()) {
            throw new QueueReadException(queueName, "data map is closed or null");
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
                MVMap<String, BufferEntry> readBufferMap = ctx.getReadBufferMap();
                if (readBufferMap != null && !readBufferMap.isClosed()) {
                    readBufferMap.put(consumerId, new BufferEntry(queueDataList, System.currentTimeMillis()));
                    statsTracker.keepRecord(consumerId, ActionType.BUFFER_INPUT);
                }
            }

            if (isBulk && customConfigVo.isBulkCommit()) {
                storeInfo.getStore().commit();
                isCommitted = true;
            }

            statsTracker.keepRecord(consumerId, queueDataList.size(), ActionType.OUTPUT);
            return queueDataList;

        } catch (QueueReadException e) {
            logger.error("read failed queue={} consumer={} count={}", queueName, consumerId, requestCount, e);
            throw new QueueReadException(queueName, e.getMessage(), e);
        } catch (Exception e) {
            if (isCommitted) {
                try {
                    storeInfo.getStore().rollback();
                } catch (Exception e1) {
                    e.addSuppressed(e1);
                }
            }
            logger.error("read failed queue={} consumer={} count={}", queueName, consumerId, requestCount, e);
            throw new QueueReadException(queueName, "read failed", e);
        }finally {
            writeLock.unlock();
        }
    }

    private List<FileQueueData> extractData(QueueContext ctx, int selectCount) throws QueueException {
        logger.debug("extractData count={}", selectCount);
        List<FileQueueData> queueDataList = new ArrayList<>(selectCount);
        try {
            queueManager.realignDataKeyIfNeeded();  // v2.1.0: Performance optimization

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
            logger.error("extractData failed queue={}", storeInfo.getCONFIG().getQueueName(), e);
            throw new QueueReadException(storeInfo.getCONFIG().getQueueName(), "extract data failed", e);
        }
    }

    private List<FileQueueData> readBuffer(String consumerId) throws QueueException {
        statsTracker.keepRecord(consumerId, ActionType.BUFFER_OUTPUT_INVOKE);
        logger.debug("readBuffer executor={}", consumerId);

        QueueContext ctx = queueManager.getQueueContext();
        MVMap<String, BufferEntry> readBufferMap = ctx.getReadBufferMap();
        if (readBufferMap == null || readBufferMap.isClosed()) {
           return null;
        }

        ReentrantReadWriteLock.ReadLock readLock = ctx.getLock().readLock();
        readLock.lock();
        try {
            BufferEntry entry = readBufferMap.get(consumerId);
            if (entry == null) {
                return null;
            }
            FileQueueCustomConfigVo customConfigVo = storeInfo.getCONFIG().getCustomConfig();
            long ttlMinutes = customConfigVo.getBufferTtlMinutes();
            long now = System.currentTimeMillis();
            if (ttlMinutes > 0 && now - entry.getLastAccessEpochMillis() > ttlMinutes * 60_000L) {
                logger.warn("buffer ttl exceeded queue={} consumer={} size={}",
                        storeInfo.getCONFIG().getQueueName(),
                        consumerId,
                        entry.getData() == null ? 0 : entry.getData().size());
            }
            // 갱신된 접근 시각으로 저장(선택)
            readBufferMap.put(consumerId, entry.touch());
            return entry.getData();
        } catch (Exception e) {
            logger.error("readBuffer failed queue={} consumer={}", storeInfo.getCONFIG().getQueueName(), consumerId, e);
            throw new QueueReadException(storeInfo.getCONFIG().getQueueName(), "buffer read failed for consumer " + consumerId, e);
        } finally {
            readLock.unlock();
        }
    }

    public void readCommit(String consumerId) throws QueueException {
        final String queueName = storeInfo.getCONFIG().getQueueName();
        if (storeInfo.getStoreOpenTime() == null) {
            throw new QueueReadException(queueName, "store not opened; call open() first");
        }

        QueueContext ctx = queueManager.getQueueContext();
        MVMap<String, BufferEntry> readBufferMap = ctx.getReadBufferMap();
        if (readBufferMap == null || readBufferMap.isClosed()) {
            return;
        }

        ReentrantReadWriteLock.WriteLock writeLock = ctx.getLock().writeLock();
        writeLock.lock();
        try {
            logger.debug("readCommit executor={}", consumerId);
            readBufferMap.remove(consumerId);
            statsTracker.keepRecord(consumerId, ActionType.BUFFER_OUTPUT);
        } catch (Exception e) {
            logger.error("readCommit failed queue={} consumer={}", queueName, consumerId, e);
            throw new QueueReadException(queueName, "buffer commit failed for consumer " + consumerId, e);
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
            Map<String, List<FileQueueData>> result = new HashMap<>();
            for (Map.Entry<String, BufferEntry> entry : ctx.getReadBufferMap().entrySet()) {
                List<FileQueueData> data = entry.getValue() == null ? null : entry.getValue().getData();
                result.put(entry.getKey(), data == null ? Collections.emptyList() : new ArrayList<>(data));
            }
            return result;
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
    public List<FileQueueData> getBuffer(String consumerId) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().readLock().lock();
        try {
            MVMap<String, BufferEntry> map = ctx.getReadBufferMap();
            if (map == null) {
                return Collections.emptyList();
            }
            BufferEntry entry = map.get(consumerId);
            List<FileQueueData> list = entry == null ? null : entry.getData();
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
    public void clearBuffer(String consumerId) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().writeLock().lock();
        try {
            if (ctx.getReadBufferMap() != null) {
                ctx.getReadBufferMap().remove(consumerId);
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
    public void removeOne(String consumerId) {
        QueueContext ctx = queueManager.getQueueContext();
        ctx.getLock().writeLock().lock();
        try {
            MVMap<String, BufferEntry> bufferMap = ctx.getReadBufferMap();
            if (bufferMap != null && bufferMap.containsKey(consumerId)) {
                bufferMap.remove(consumerId);
                return;
            }

            MVMap<Long, FileQueueData> dataMap = ctx.getDataMap();
            if (dataMap == null || dataMap.isEmpty()) {
                return;
            }

            ConcurrentSkipListSet<Long> keyList = ctx.getTransactionKeyList();
            if (keyList != null) {
                try {
                    queueManager.realignDataKeyIfNeeded();  // v2.1.0: Performance optimization
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
