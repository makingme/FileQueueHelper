package com.queue.file.vo;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @since : 2025-07-07(월)
 */
public class InOutStorage {

    // 처리 함수 호출 이력
    private final Map<String, LocalDateTime> outputInvokeHistoryMap = new ConcurrentHashMap<>();
    private final Map<String, LocalDateTime> bufferOutputInvokeHistoryMap = new ConcurrentHashMap<>();

    // 유입 건수
    private final Map<String, LocalDateTime> inputHistoryMap = new ConcurrentHashMap<>();
    private final AtomicLong inputCount = new AtomicLong(0);

    // 처리 건수
    private final Map<String, LocalDateTime> outputHistoryMap = new ConcurrentHashMap<>();
    private final AtomicLong outputCount = new AtomicLong(0);

    // 버퍼 유입 건수
    private final Map<String, LocalDateTime> bufferInputHistoryMap = new ConcurrentHashMap<>();
    private final AtomicLong bufferInputCount = new AtomicLong(0);
    
    // 버퍼 처리 건수
    private final Map<String, LocalDateTime> bufferOutputHistoryMap = new ConcurrentHashMap<>();
    private final AtomicLong bufferOutputCount = new AtomicLong(0);

    public void recordOutputInvokeHistory(String executorName){
        outputInvokeHistoryMap.put(executorName, LocalDateTime.now());
    }
    public Map<String, LocalDateTime> getOutputInvokeHistoryMap() {
        return Collections.unmodifiableMap(outputInvokeHistoryMap);
    }

    public void recordBufferOutputInvokeHistory(String executorName){
        bufferOutputInvokeHistoryMap.put(executorName, LocalDateTime.now());
    }
    public Map<String, LocalDateTime> getBufferOutputInvokeHistoryMap() {
        return Collections.unmodifiableMap(bufferOutputInvokeHistoryMap);
    }

    public Long getInputCount() { return inputCount.get(); }
    public void addInputCount(String executorName, Long count) {
        inputCount.updateAndGet(currentValue -> {
            if (currentValue >= Long.MAX_VALUE - count) {
                return count;
            }
            inputHistoryMap.put(executorName, LocalDateTime.now());
            return currentValue + count;
        });
    }

    public Long getOutputCount() { return outputCount.get(); }
    public void addOutputCount(String executorName, Long count) {
        outputCount.updateAndGet(currentValue -> {
            if (currentValue >= Long.MAX_VALUE - count) {
                return count;
            }
            outputHistoryMap.put(executorName, LocalDateTime.now());
            return currentValue + count;
        });
    }

    public Long getBufferInputCount() { return bufferInputCount.get(); }
    public void addBufferInputCount(String executorName, Long count) {
        bufferInputCount.updateAndGet(currentValue -> {
            if (currentValue >= Long.MAX_VALUE - count) {
                return count;
            }
            bufferInputHistoryMap.put(executorName, LocalDateTime.now());
            return currentValue + count;
        });
    }

    public Long getBufferOutputCount() { return bufferOutputCount.get(); }
    public void addBufferOutputCount(String executorName, Long count) {
        bufferOutputCount.updateAndGet(currentValue -> {
            if (currentValue >= Long.MAX_VALUE - count) {
                return count;
            }
            bufferOutputHistoryMap.put(executorName, LocalDateTime.now());
            return currentValue + count;
        });
    }
}
