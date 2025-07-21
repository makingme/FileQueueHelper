package com.queue.file.controller;

import com.queue.file.vo.ActionType;
import com.queue.file.vo.StoreInfo;
import com.queue.file.vo.InOutStorage;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @since : 2025-07-16(수)
 */
public class StatsTracker {
    private final Map<String, InOutStorage> parttitionInOutInfoMap = new ConcurrentHashMap<>();

    // 유입 건수
    private final AtomicLong TOTAL_INPUT_COUNT = new AtomicLong(0);
    // 처리 건수
    private final AtomicLong TOTAL_OUTPUT_COUNT = new AtomicLong(0);

    public void keepRecord(String partitionName, String executorName, ActionType actionType) {
        keepRecord(partitionName, executorName, 1, actionType);
    }

    public void keepRecord(String partitionName, String executorName, long count, ActionType actionType) {
        InOutStorage ioStorage = parttitionInOutInfoMap.computeIfAbsent(partitionName, k -> new InOutStorage());
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
        return parttitionInOutInfoMap.computeIfAbsent(partitionName, k -> new InOutStorage());
    }

    public Map<String, InOutStorage> getParttitionInOutInfoMap() { return Collections.unmodifiableMap(parttitionInOutInfoMap); }

    public Long getTOTAL_INPUT_COUNT() { return TOTAL_INPUT_COUNT.get(); }
    public void addTOTAL_INPUT_COUNT(Long count) {
        TOTAL_INPUT_COUNT.updateAndGet(currentValue -> {
            if (currentValue >= Long.MAX_VALUE - count) {
                return count;
            }
            return currentValue + count;
        });
    }
    public Long getTOTAL_OUTPUT_COUNT() { return TOTAL_OUTPUT_COUNT.get(); }
    public void addTOTAL_OUTPUT_COUNT(Long count) {
        TOTAL_OUTPUT_COUNT.updateAndGet(currentValue -> {
            if (currentValue >= Long.MAX_VALUE - count) {
                return count;
            }
            return currentValue + count;
        });
    }
}

