package com.queue.file.controller;

import com.queue.file.exception.*;
import com.queue.file.utils.Contents;
import com.queue.file.vo.FileQueueData;
import com.queue.file.vo.PartitionContext;
import com.queue.file.vo.PartitionSummaryVo;
import com.queue.file.vo.StoreInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @since : 2025-07-16(수)
 */
public class BaseController {
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
        try {
            dataAccess.write("", Contents.DEFAULT_PARTITION, Thread.currentThread().getName(), data);
        }catch (QueueException e){
            throw new QueueWriteException(e);
        }
    }

    public void write(String partition, String data) throws QueueWriteException {
        try {
            dataAccess.write("", partition, Thread.currentThread().getName(), data);
        }catch (QueueException e){
            throw new QueueWriteException(e);
        }
    }

    public void write(String partition, String executorName, String data) throws QueueWriteException {
        try {
            dataAccess.write("", partition, executorName, data);
        }catch (QueueException e){
            throw new QueueWriteException(e);
        }
    }

    public void write(String tag, String partition, String executorName, String data) throws QueueWriteException {
        try {
            dataAccess.write(tag, partition, executorName, data);
        }catch (QueueException e){
            throw new QueueWriteException(e);
        }
    }

    public void writeBulk(List<String> dataList) throws QueueWriteException {
        try {
            dataAccess.writeBulk("", Contents.DEFAULT_PARTITION, Thread.currentThread().getName(), dataList);
        }catch (QueueException e){
            throw new QueueWriteException(e);
        }
    }

    public void writeBulk(String partition, List<String> dataList) throws QueueWriteException {
        try {
            dataAccess.writeBulk("", partition, Thread.currentThread().getName(), dataList);
        }catch (QueueException e){
            throw new QueueWriteException(e);
        }
    }

    public void writeBulk(String partition, String executorName, List<String> dataList) throws QueueWriteException {
        try {
            dataAccess.writeBulk("", partition, executorName, dataList);
        }catch (QueueException e){
            throw new QueueWriteException(e);
        }
    }

    public void writeBulk(String tag, String partition, String executorName, List<String> dataList) throws QueueWriteException {
        try {
            dataAccess.writeBulk(tag, partition, executorName, dataList);
        }catch (QueueException e){
            throw new QueueWriteException(e);
        }
    }

    public void writeQueueData(String executorName, List<FileQueueData> dataList) throws QueueWriteException {
        try {
            dataAccess.writeQueueData(executorName, dataList);
        }catch (QueueException e){
            throw new QueueWriteException(e);
        }
    }

    public FileQueueData read() throws QueueReadException {
        try {
            return dataAccess.read(Contents.DEFAULT_PARTITION, Thread.currentThread().getName());
        }catch (QueueException e){
            throw new QueueReadException(e);
        }
    }

    public FileQueueData read(String executorName) throws QueueReadException {
        try {
            return dataAccess.read(Contents.DEFAULT_PARTITION, executorName);
        }catch (QueueException e){
            throw new QueueReadException(e);
        }
    }

    public FileQueueData read(String partitionName, String executorName) throws QueueReadException {
        try {
            return dataAccess.read(partitionName, executorName);
        }catch (QueueException e){
            throw new QueueReadException(e);
        }
    }

    public List<FileQueueData> read(String executorName, int requestCount) throws QueueReadException {
        try {
            return dataAccess.read(Contents.DEFAULT_PARTITION, executorName, requestCount);
        }catch (QueueException e){
            throw new QueueReadException(e);
        }
    }

    public List<FileQueueData> read(String partitionName, String executorName, int requestCount) throws QueueReadException {
        try {
            return dataAccess.read(partitionName, executorName, requestCount);
        }catch (QueueException e){
            throw new QueueReadException(e);
        }
    }

    public void readCommit(String executorName) throws QueueReadException {
        try {
            dataAccess.readCommit(Contents.DEFAULT_PARTITION, executorName);
        }catch (QueueException e){
            throw new QueueReadException(e);
        }
    }

    public void readCommit(String partitionName, String executorName) throws QueueReadException {
        try {
            dataAccess.readCommit(partitionName, executorName);
        }catch (QueueException e){
            throw new QueueReadException(e);
        }
    }

    public void checkAllState() throws UnsteadyStateException {
        partitionManager.checkAllState();
    }

    public void checkState() throws UnsteadyStateException {
        checkState(Contents.DEFAULT_PARTITION);
    }

    public void checkState(String partitionName) throws UnsteadyStateException {
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
        dataAccess.clearAllData();
    }

    // 특정 파티션의 전체 데이터 정보 삭제 함수
    public void clearData(String partitionName) {
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
        dataAccess.clearAllBuffer();
    }

    // 특정 파티션의 모든 버퍼 정보 삭제 함수
    public void clearBuffer(String partitionName) {
        dataAccess.clearBuffer(partitionName);
    }

    // 특정 파티션의 특정 버퍼 정보 삭제 함수
    public void clearBuffer(String partitionName, String executorName) {
        dataAccess.clearBuffer(partitionName, executorName);
    }

    // 특정 파티션의 특정 캐시 정보 가져오는 함수
    public Object getCache(String partitionName, String cacheKey) {
        return dataAccess.getCache(partitionName, cacheKey);
    }

    // 모든 파티션의 모든 캐시 정보 삭제 함수
    public void clearAllCache() {
        dataAccess.clearAllCache();
    }

    // 특정 파티션의 모든 캐시 정보 삭제 함수
    public void clearCache(String partitionName) {
        dataAccess.clearCache(partitionName);
    }

    // 특정 파티션의 특정 캐시 정보 삭제 함수
    public void clearCache(String partitionName, String cacheKey) {
        dataAccess.clearCache(partitionName, cacheKey);
    }
}
