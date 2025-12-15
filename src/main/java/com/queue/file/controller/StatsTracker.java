package com.queue.file.controller;

import com.queue.file.vo.ActionType;
import com.queue.file.vo.InOutStorage;

import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * v3.0.0: Single queue stats tracking (partition concept removed)
 * @since : 2025-07-16(수)
 */
public class StatsTracker {
    private static final Logger logger = LoggerFactory.getLogger(StatsTracker.class);
    private final InOutStorage inOutStorage = new InOutStorage();

    // 유입 건수
    private final LongAdder TOTAL_INPUT_COUNT = new LongAdder();
    // 처리 건수
    private final LongAdder TOTAL_OUTPUT_COUNT = new LongAdder();

    public void keepRecord(String executorId, ActionType actionType) {
        keepRecord(executorId, 1, actionType);
    }

    public void keepRecord(String executorId, long count, ActionType actionType) {
        logger.debug("keepRecord executor={} action={}", executorId, actionType);
        switch (actionType) {
            case INPUT:
                addTOTAL_INPUT_COUNT(count);
                inOutStorage.addInputCount(executorId, count);
                break;
            case OUTPUT:
                addTOTAL_OUTPUT_COUNT(count);
                inOutStorage.addOutputCount(executorId, count);
                break;
            case OUTPUT_INVOKE:
                inOutStorage.recordOutputInvokeHistory(executorId);
                break;
            case BUFFER_INPUT:
                inOutStorage.addBufferInputCount(executorId, count);
                break;
            case BUFFER_OUTPUT:
                inOutStorage.addBufferOutputCount(executorId, count);
                break;
            case BUFFER_OUTPUT_INVOKE:
                inOutStorage.recordBufferOutputInvokeHistory(executorId);
                break;
        }
    }

    public InOutStorage getInOutInfo() {
        return inOutStorage;
    }

    public Long getTOTAL_INPUT_COUNT() { return TOTAL_INPUT_COUNT.sum(); }
    public void addTOTAL_INPUT_COUNT(long count) {
        TOTAL_INPUT_COUNT.add(count);
    }
    public Long getTOTAL_OUTPUT_COUNT() { return TOTAL_OUTPUT_COUNT.sum(); }
    public void addTOTAL_OUTPUT_COUNT(long count) {
        TOTAL_OUTPUT_COUNT.add(count);
    }
}

