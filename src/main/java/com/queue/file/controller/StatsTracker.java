package com.queue.file.controller;

import com.queue.file.vo.ActionType;
import com.queue.file.vo.InOutStorage;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since : 2025-07-16(수)
 */
public class StatsTracker {
    private static final Logger logger = LoggerFactory.getLogger(StatsTracker.class);
    private final Map<String, InOutStorage> partitionInOutInfoMap = new ConcurrentHashMap<>();

    // 유입 건수
    private final LongAdder TOTAL_INPUT_COUNT = new LongAdder();
    // 처리 건수
    private final LongAdder TOTAL_OUTPUT_COUNT = new LongAdder();

    public void keepRecord(String partitionName, String executorName, ActionType actionType) {
        keepRecord(partitionName, executorName, 1, actionType);
    }

    public void keepRecord(String partitionName, String executorName, long count, ActionType actionType) {
        logger.debug("keepRecord partition={} executor={} action={}", partitionName, executorName, actionType);
        InOutStorage ioStorage = getOrCreateStorage(partitionName);
        switch (actionType) {
            case INPUT:
                addTOTAL_INPUT_COUNT(count);
                ioStorage.addInputCount(executorName, count);
                break;
            case OUTPUT:
                addTOTAL_OUTPUT_COUNT(count);
                ioStorage.addOutputCount(executorName, count);
                break;
            case OUTPUT_INVOKE:
                ioStorage.recordOutputInvokeHistory(executorName);
                break;
            case BUFFER_INPUT:
                ioStorage.addBufferInputCount(executorName, count);
                break;
            case BUFFER_OUTPUT:
                ioStorage.addBufferOutputCount(executorName, count);
                break;
            case BUFFER_OUTPUT_INVOKE:
                ioStorage.recordBufferOutputInvokeHistory(executorName);
                break;
        }
    }

    public InOutStorage getInOutInfo(String partitionName) {
        return getOrCreateStorage(partitionName);
    }

    private InOutStorage getOrCreateStorage(String partitionName) {
        return partitionInOutInfoMap.computeIfAbsent(partitionName, k -> new InOutStorage());
    }

    public Map<String, InOutStorage> getPartitionInOutInfoMap() { return Collections.unmodifiableMap(partitionInOutInfoMap); }

    public Long getTOTAL_INPUT_COUNT() { return TOTAL_INPUT_COUNT.sum(); }
    public void addTOTAL_INPUT_COUNT(long count) {
        TOTAL_INPUT_COUNT.add(count);
    }
    public Long getTOTAL_OUTPUT_COUNT() { return TOTAL_OUTPUT_COUNT.sum(); }
    public void addTOTAL_OUTPUT_COUNT(long count) {
        TOTAL_OUTPUT_COUNT.add(count);
    }
}

