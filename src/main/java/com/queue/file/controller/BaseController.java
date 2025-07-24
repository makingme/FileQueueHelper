package com.queue.file.controller;

import com.queue.file.exception.QueueException;
import com.queue.file.exception.QueueReadException;
import com.queue.file.exception.QueueWriteException;
import com.queue.file.exception.UnsteadyStateException;
import com.queue.file.utils.Contents;
import com.queue.file.vo.FileQueueData;
import com.queue.file.vo.PartitionContext;
import com.queue.file.vo.PartitionSummaryVo;
import com.queue.file.vo.StoreInfo;
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
    private final PartitionManager partitionManager;
    private final DataAccess dataAccess;

    BaseController(StoreInfo storeInfo) {
        this.storeInfo = storeInfo;
        this.partitionManager = new PartitionManager(storeInfo);
        this.dataAccess = new DataAccess(storeInfo, partitionManager);
    }

    public StoreInfo getStoreInfo() { return storeInfo; }

    public void write(String data) throws QueueWriteException {
        logger.debug("write default partition data: {}", data);
        try {
            dataAccess.write("", Contents.DEFAULT_PARTITION, Thread.currentThread().getName(), data);
        }catch (QueueException e){
            logger.error("Write failed", e);
            throw new QueueWriteException(e);
        }
    }

    public void write(String partition, String data) throws QueueWriteException {
        logger.debug("write data: partition={}, data={}", partition, data);
        try {
            dataAccess.write("", partition, Thread.currentThread().getName(), data);
        }catch (QueueException e){
            logger.error("Write failed", e);
            throw new QueueWriteException(e);
        }
    }

    public void write(String partition, String executorName, String data) throws QueueWriteException {
        logger.debug("write data: partition={}, executor={}, data={}", partition, executorName, data);
        try {
            dataAccess.write("", partition, executorName, data);
        }catch (QueueException e){
            logger.error("Write failed", e);
            throw new QueueWriteException(e);
        }
    }

    public void write(String tag, String partition, String executorName, String data) throws QueueWriteException {
        logger.debug("write data: tag={}, partition={}, executor={}", tag, partition, executorName);
        try {
            dataAccess.write(tag, partition, executorName, data);
        }catch (QueueException e){
            logger.error("Write failed", e);
            throw new QueueWriteException(e);
        }
    }

    public void writeBulk(List<String> dataList) throws QueueWriteException {
        logger.debug("write bulk default partition size={}", dataList==null?0:dataList.size());
        try {
            dataAccess.writeBulk("", Contents.DEFAULT_PARTITION, Thread.currentThread().getName(), dataList);
        }catch (QueueException e){
            logger.error("Bulk write failed", e);
            throw new QueueWriteException(e);
        }
    }

    public void writeBulk(String partition, List<String> dataList) throws QueueWriteException {
        logger.debug("write bulk: partition={}, size={}", partition, dataList==null?0:dataList.size());
        try {
            dataAccess.writeBulk("", partition, Thread.currentThread().getName(), dataList);
        }catch (QueueException e){
            logger.error("Bulk write failed", e);
            throw new QueueWriteException(e);
        }
    }

    public void writeBulk(String partition, String executorName, List<String> dataList) throws QueueWriteException {
        logger.debug("write bulk: partition={}, executor={}, size={}", partition, executorName, dataList==null?0:dataList.size());
        try {
            dataAccess.writeBulk("", partition, executorName, dataList);
        }catch (QueueException e){
            logger.error("Bulk write failed", e);
            throw new QueueWriteException(e);
        }
    }

    public void writeBulk(String tag, String partition, String executorName, List<String> dataList) throws QueueWriteException {
        logger.debug("write bulk: tag={}, partition={}, executor={}, size={}", tag, partition, executorName, dataList==null?0:dataList.size());
        try {
            dataAccess.writeBulk(tag, partition, executorName, dataList);
        }catch (QueueException e){
            logger.error("Bulk write failed", e);
            throw new QueueWriteException(e);
        }
    }

    public void writeQueueData(String executorName, List<FileQueueData> dataList) throws QueueWriteException {
        logger.debug("write queue data: executor={}, size={}", executorName, dataList==null?0:dataList.size());
        try {
            dataAccess.writeQueueData(executorName, dataList);
        }catch (QueueException e){
            logger.error("Write queue data failed", e);
            throw new QueueWriteException(e);
        }
    }

    public FileQueueData read() throws QueueReadException {
        logger.debug("read default partition");
        try {
            return dataAccess.read(Contents.DEFAULT_PARTITION, Thread.currentThread().getName());
        }catch (QueueException e){
            logger.error("Read failed", e);
            throw new QueueReadException(e);
        }
    }

    public FileQueueData read(String executorName) throws QueueReadException {
        logger.debug("read default partition executor={}", executorName);
        try {
            return dataAccess.read(Contents.DEFAULT_PARTITION, executorName);
        }catch (QueueException e){
            logger.error("Read failed", e);
            throw new QueueReadException(e);
        }
    }

    public FileQueueData read(String partitionName, String executorName) throws QueueReadException {
        logger.debug("read partition={}, executor={}", partitionName, executorName);
        try {
            return dataAccess.read(partitionName, executorName);
        }catch (QueueException e){
            logger.error("Read failed", e);
            throw new QueueReadException(e);
        }
    }

    public List<FileQueueData> read(String executorName, int requestCount) throws QueueReadException {
        logger.debug("read executor={} count={}", executorName, requestCount);
        try {
            return dataAccess.read(Contents.DEFAULT_PARTITION, executorName, requestCount);
        }catch (QueueException e){
            logger.error("Read failed", e);
            throw new QueueReadException(e);
        }
    }

    public List<FileQueueData> read(String partitionName, String executorName, int requestCount) throws QueueReadException {
        logger.debug("read partition={}, executor={}, count={}", partitionName, executorName, requestCount);
        try {
            return dataAccess.read(partitionName, executorName, requestCount);
        }catch (QueueException e){
            logger.error("Read failed", e);
            throw new QueueReadException(e);
        }
    }

    public void readCommit(String executorName) throws QueueReadException {
        logger.debug("readCommit executor={} default partition", executorName);
        try {
            dataAccess.readCommit(Contents.DEFAULT_PARTITION, executorName);
        }catch (QueueException e){
            logger.error("ReadCommit failed", e);
            throw new QueueReadException(e);
        }
    }

    public void readCommit(String partitionName, String executorName) throws QueueReadException {
        logger.debug("readCommit partition={}, executor={}", partitionName, executorName);
        try {
            dataAccess.readCommit(partitionName, executorName);
        }catch (QueueException e){
            logger.error("ReadCommit failed", e);
            throw new QueueReadException(e);
        }
    }

    public void checkAllState() throws UnsteadyStateException {
        logger.debug("check all state");
        partitionManager.checkAllState();
    }

    public void checkState() throws UnsteadyStateException {
        logger.debug("check state default partition");
        checkState(Contents.DEFAULT_PARTITION);
    }

    public void checkState(String partitionName) throws UnsteadyStateException {
        logger.debug("check state partition={}", partitionName);
        partitionManager.checkState(partitionName, true);
    }

    // 전체 파티션 목록 정보 가져 오는 함수
    public Set<String> getAllPartitionNameSet(){
        return partitionManager.getPartitionContextMap().keySet();
    }

    // 요약 정보 가져오는 함수 - 파티션 별 - 영역 별 데이터 갯수
    public Map<String, PartitionSummaryVo> getSummaryInfo() {
        return dataAccess.getSummaryInfo();
    }

    // 특정 파티션 데이터 목록 가져 오는 함수
    public List<FileQueueData> getPartitionDataList(String partitionName) {
        return dataAccess.getPartitionDataList(partitionName);
    }

    // 특정 파티션 버퍼 목록 가져 오는 함수
    public Map<String, List<FileQueueData>> getPartitionBufferList(String partitionName) {
        return dataAccess.getPartitionBufferList(partitionName);
    }

    // 특정 파티션 캐시 목록 가져 오는 함수
    public Map<String, Object> getPartitionCacheList(String partitionName) {
        return dataAccess.getPartitionCacheList(partitionName);
    }

    // 모든 파티션의 전체 데이터 정보 가져 오는 함수
    public Map<String, List<FileQueueData>> getAllDataList() {
        return dataAccess.getAllDataList();
    }

    // 특정 파티션의 특정 데이터 정보 가져 오는 함수
    public FileQueueData getData(String partitionName, Long transactionKey) {
        return dataAccess.getData(partitionName, transactionKey);
    }

    // 모든 파티션의 전체 데이터 정보 삭제 함수
    public void clearAllData() {
        logger.debug("clear all data");
        dataAccess.clearAllData();
    }

    // 특정 파티션의 전체 데이터 정보 삭제 함수
    public void clearData(String partitionName) {
        logger.debug("clear data partition={}", partitionName);
        dataAccess.clearData(partitionName);
    }

    // 특정 파티션의 특정 데이터 정보 삭제 함수
    public void removeData(String partitionName, Long transactionKey) {
        dataAccess.removeData(partitionName, transactionKey);
    }

    // 특정 파티션의 특정 버퍼 정보 가져오는 함수
    public List<FileQueueData> getBuffer(String partitionName, String executorName) {
        return dataAccess.getBuffer(partitionName, executorName);
    }

    // 모든 파티션의 모든 버퍼 정보 삭제 함수
    public void clearAllBuffer() {
        logger.debug("clear all buffer");
        dataAccess.clearAllBuffer();
    }

    // 특정 파티션의 모든 버퍼 정보 삭제 함수
    public void clearBuffer(String partitionName) {
        logger.debug("clear buffer partition={}", partitionName);
        dataAccess.clearBuffer(partitionName);
    }

    // 특정 파티션의 특정 버퍼 정보 삭제 함수
    public void clearBuffer(String partitionName, String executorName) {
        logger.debug("clear buffer partition={} executor={}", partitionName, executorName);
        dataAccess.clearBuffer(partitionName, executorName);
    }

    // 특정 파티션의 특정 캐시 정보 가져오는 함수
    public Object getCache(String partitionName, String cacheKey) {
        return dataAccess.getCache(partitionName, cacheKey);
    }

    // 모든 파티션의 모든 캐시 정보 삭제 함수
    public void clearAllCache() {
        logger.debug("clear all cache");
        dataAccess.clearAllCache();
    }

    // 특정 파티션의 모든 캐시 정보 삭제 함수
    public void clearCache(String partitionName) {
        logger.debug("clear cache partition={}", partitionName);
        dataAccess.clearCache(partitionName);
    }

    // 특정 파티션의 특정 캐시 정보 삭제 함수
    public void clearCache(String partitionName, String cacheKey) {
        logger.debug("clear cache partition={} key={}", partitionName, cacheKey);
        dataAccess.clearCache(partitionName, cacheKey);
    }

    // 지정 파티션 데이터 또는 버퍼에서 한 건 삭제
    public void removeOne(String partitionName, String executorName) {
        logger.debug("remove one partition={} executor={}", partitionName, executorName);
        dataAccess.removeOne(partitionName, executorName);
    }

    @Override
    public void close() {
        logger.info("AutoCloseable#close: Graceful shutdown start");

        List<ReentrantReadWriteLock.WriteLock> acquiredLocks = new ArrayList<>();
        try {
            List<String> partitions = new ArrayList<>(partitionManager.getPartitionContextMap().keySet());
            Collections.sort(partitions);

            for (String partition : partitions) {
                PartitionContext ctx = partitionManager.getPartitionContextMap().get(partition);
                ReentrantReadWriteLock.WriteLock wl = ctx.getLock().writeLock();

                boolean locked = false;
                final int maxRetry = 3;
                final long timeoutPerTryMillis = 5000L;

                for (int i = 0; i < maxRetry; i++) {
                    try {
                        if (wl.tryLock(timeoutPerTryMillis, TimeUnit.MILLISECONDS)) {
                            acquiredLocks.add(wl);
                            logger.debug("Lock acquired for partition {} (attempt {})", partition, i + 1);
                            locked = true;
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.warn("Interrupted while locking {}", partition);
                        break;
                    }
                }

                if (!locked) {
                    logger.error("Failed to lock partition {} after retries", partition);
                }
            }

            if (storeInfo.getStore() != null && !storeInfo.getStore().isClosed()) {
                storeInfo.getStore().commit();
                storeInfo.getStore().close();
                storeInfo.setStoreOpenTime(null);
                logger.info("store committed and closed");
            }

        } catch (Exception e) {
            logger.error("Error during graceful close", e);
        } finally {
            for (int i = acquiredLocks.size() - 1; i >= 0; i--) {
                try {
                    acquiredLocks.get(i).unlock();
                } catch (Exception ignore) {}
            }
        }
    }
}
