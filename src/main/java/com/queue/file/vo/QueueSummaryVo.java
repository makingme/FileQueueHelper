package com.queue.file.vo;

/**
 * Summary information for single queue (v3.0.0)
 * Replaces PartitionSummaryVo
 */
public class QueueSummaryVo {
    private final int dataCount;
    private final int bufferCount;
    private final int cacheCount;

    public QueueSummaryVo(int dataCount, int bufferCount, int cacheCount) {
        this.dataCount = dataCount;
        this.bufferCount = bufferCount;
        this.cacheCount = cacheCount;
    }

    public int getDataCount() {
        return dataCount;
    }

    public int getBufferCount() {
        return bufferCount;
    }

    public int getCacheCount() {
        return cacheCount;
    }

    @Override
    public String toString() {
        return "QueueSummaryVo{" +
                "dataCount=" + dataCount +
                ", bufferCount=" + bufferCount +
                ", cacheCount=" + cacheCount +
                '}';
    }
}
