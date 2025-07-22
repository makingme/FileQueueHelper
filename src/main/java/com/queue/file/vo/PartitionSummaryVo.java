package com.queue.file.vo;

/**
 * Simple summary information for a partition.
 */
public class PartitionSummaryVo {
    private final String partitionName;
    private final int dataCount;
    private final int bufferCount;
    private final int cacheCount;

    public PartitionSummaryVo(String partitionName, int dataCount, int bufferCount, int cacheCount) {
        this.partitionName = partitionName;
        this.dataCount = dataCount;
        this.bufferCount = bufferCount;
        this.cacheCount = cacheCount;
    }

    public String getPartitionName() {
        return partitionName;
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
}
