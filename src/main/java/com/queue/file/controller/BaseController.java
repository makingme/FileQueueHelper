package com.queue.file.controller;

import com.queue.file.exception.*;
import com.queue.file.utils.Contents;
import com.queue.file.vo.FileQueueCustomConfigVo;
import com.queue.file.vo.FileQueueData;
import com.queue.file.vo.QueueContext;
import com.queue.file.vo.QueueSummaryVo;
import com.queue.file.vo.StoreInfo;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @since : 2025-07-16(수)
 */
public class BaseController implements AutoCloseable{
    private static final Logger logger = LoggerFactory.getLogger(BaseController.class);
    private final StoreInfo storeInfo;
    private final QueueManager queueManager;
    private final DataAccess dataAccess;

    BaseController(StoreInfo storeInfo) throws InitializeException {
        this.storeInfo = storeInfo;
        this.queueManager = new QueueManager(storeInfo);
        this.dataAccess = new DataAccess(storeInfo, queueManager);
    }

    public StoreInfo getStoreInfo() { return storeInfo; }

    public void write(String data) throws QueueWriteException {
        logger.debug("write data: {}", data);
        try {
            dataAccess.write("", Thread.currentThread().getName(), data);
        }catch (QueueException e){
            logger.error("Write failed", e);
            throw new QueueWriteException(storeInfo.getCONFIG().getQueueName(), "write failed", e);
        }
    }

    public void write(String tag, String producerId, String data) throws QueueWriteException {
        logger.debug("write data: tag={}, executor={}", tag, producerId);
        try {
            dataAccess.write(tag, producerId, data);
        }catch (QueueException e){
            logger.error("Write failed", e);
            throw new QueueWriteException(storeInfo.getCONFIG().getQueueName(), "write failed", e);
        }
    }

    public void writeBulk(List<String> dataList) throws QueueWriteException {
        logger.debug("write bulk size={}", dataList==null?0:dataList.size());
        try {
            dataAccess.writeBulk("", Thread.currentThread().getName(), dataList);
        }catch (QueueException e){
            logger.error("Bulk write failed", e);
            throw new QueueWriteException(storeInfo.getCONFIG().getQueueName(), "bulk write failed", e);
        }
    }

    public void writeBulk(String tag, String producerId, List<String> dataList) throws QueueWriteException {
        logger.debug("write bulk: tag={}, executor={}, size={}", tag, producerId, dataList==null?0:dataList.size());
        try {
            dataAccess.writeBulk(tag, producerId, dataList);
        }catch (QueueException e){
            logger.error("Bulk write failed", e);
            throw new QueueWriteException(storeInfo.getCONFIG().getQueueName(), "bulk write failed", e);
        }
    }

    public void writeQueueData(String producerId, List<FileQueueData> dataList) throws QueueWriteException {
        logger.debug("write queue data: executor={}, size={}", producerId, dataList==null?0:dataList.size());
        try {
            dataAccess.writeQueueData(producerId, dataList);
        }catch (QueueException e){
            logger.error("Write queue data failed", e);
            throw new QueueWriteException(storeInfo.getCONFIG().getQueueName(), "write queue data failed", e);
        }
    }

    public FileQueueData read() throws QueueReadException {
        logger.debug("read");
        try {
            return dataAccess.read(Thread.currentThread().getName());
        }catch (QueueException e){
            logger.error("Read failed", e);
            throw new QueueReadException(storeInfo.getCONFIG().getQueueName(), "read failed", e);
        }
    }

    public FileQueueData read(String consumerId) throws QueueReadException {
        logger.debug("read executor={}", consumerId);
        try {
            return dataAccess.read(consumerId);
        }catch (QueueException e){
            logger.error("Read failed", e);
            throw new QueueReadException(storeInfo.getCONFIG().getQueueName(), "read failed", e);
        }
    }

    public List<FileQueueData> read(String consumerId, int requestCount) throws QueueReadException {
        logger.debug("read executor={} count={}", consumerId, requestCount);
        try {
            return dataAccess.read(consumerId, requestCount);
        }catch (QueueException e){
            logger.error("Read failed", e);
            throw new QueueReadException(storeInfo.getCONFIG().getQueueName(), "read failed", e);
        }
    }

    public void readCommit(String consumerId) throws QueueReadException {
        logger.debug("readCommit executor={}", consumerId);
        try {
            dataAccess.readCommit(consumerId);
        }catch (QueueException e){
            logger.error("ReadCommit failed", e);
            throw new QueueReadException(storeInfo.getCONFIG().getQueueName(), "readCommit failed", e);
        }
    }

    public void checkState() throws UnsteadyStateException {
        logger.debug("check state");
        queueManager.checkState();
    }

    // 요약 정보 가져오는 함수 - 영역 별 데이터 갯수
    public QueueSummaryVo getSummaryInfo() {
        return dataAccess.getSummaryInfo();
    }

    // 전체 데이터 정보 가져 오는 함수
    public List<FileQueueData> getDataList() {
        return dataAccess.getDataList();
    }

    // 특정 데이터 정보 가져 오는 함수
    public FileQueueData getData(Long transactionKey) {
        return dataAccess.getData(transactionKey);
    }

    // 버퍼 목록 가져 오는 함수
    public Map<String, List<FileQueueData>> getBufferList() {
        return dataAccess.getBufferList();
    }

    // 특정 버퍼 정보 가져오는 함수
    public List<FileQueueData> getBuffer(String consumerId) {
        return dataAccess.getBuffer(consumerId);
    }

    // 캐시 목록 가져 오는 함수
    public Map<String, Object> getCacheList() {
        return dataAccess.getCacheList();
    }

    // 특정 캐시 정보 가져오는 함수
    public Object getCache(String cacheKey) {
        return dataAccess.getCache(cacheKey);
    }

    // 전체 데이터 정보 삭제 함수
    public void clearData() {
        logger.debug("clear data");
        dataAccess.clearData();
    }

    // 특정 데이터 정보 삭제 함수
    public void removeData(Long transactionKey) {
        dataAccess.removeData(transactionKey);
    }

    // 모든 버퍼 정보 삭제 함수
    public void clearBuffer() {
        logger.debug("clear buffer");
        dataAccess.clearBuffer();
    }

    // 특정 버퍼 정보 삭제 함수
    public void clearBuffer(String consumerId) {
        logger.debug("clear buffer executor={}", consumerId);
        dataAccess.clearBuffer(consumerId);
    }

    // 모든 캐시 정보 삭제 함수
    public void clearCache() {
        logger.debug("clear cache");
        dataAccess.clearCache();
    }

    // 특정 캐시 정보 삭제 함수
    public void clearCache(String cacheKey) {
        logger.debug("clear cache key={}", cacheKey);
        dataAccess.clearCache(cacheKey);
    }

    // 데이터 또는 버퍼에서 한 건 삭제
    public void removeOne(String consumerId) {
        logger.debug("remove one executor={}", consumerId);
        dataAccess.removeOne(consumerId);
    }

    @Override
    public void close() {
        logger.info("AutoCloseable#close: Graceful shutdown start");

        QueueContext ctx = queueManager.getQueueContext();
        ReentrantReadWriteLock.WriteLock wl = ctx.getLock().writeLock();

        boolean locked = false;
        final int maxRetry = 3;
        final long timeoutPerTryMillis = 5000L;

        try {
            // 락 획득 시도
            for (int i = 0; i < maxRetry; i++) {
                try {
                    if (wl.tryLock(timeoutPerTryMillis, TimeUnit.MILLISECONDS)) {
                        logger.debug("Lock acquired for queue (attempt {})", i + 1);
                        locked = true;
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while locking queue");
                    break;
                }
            }

            if (!locked) {
                logger.error("Failed to acquire lock for graceful shutdown after {} retries", maxRetry);
                logger.warn("Proceeding with force close - data may be inconsistent if other threads are active");
            }

            // Store close (큐 + 캐시)
            storeInfo.closeAll();
            logger.info("Queue and cache stores committed and closed");

        } catch (Exception e) {
            logger.error("Error during graceful close", e);
        } finally {
            if (locked) {
                try {
                    wl.unlock();
                } catch (Exception ignore) {}
            }
        }
    }

    /**
     * v2.1.0: 캐시 Store 접근
     */
    public MVStore getCacheStore() {
        return storeInfo.getCacheStore();
    }

    /**
     * v2.1.0: 캐시 MVMap 직접 열기
     */
    public <K, V> MVMap<K, V> openCacheMap(String mapName) {
        return storeInfo.getCacheStore().openMap(mapName);
    }
}
