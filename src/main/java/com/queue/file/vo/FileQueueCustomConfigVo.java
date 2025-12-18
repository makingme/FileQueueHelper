package com.queue.file.vo;

public class FileQueueCustomConfigVo {
    private int autoCommitDelay = 1000;  // milliseconds
    private int autoCommitMemory = 19 * 1024;  // 19KB (H2 MVStore default recommendation)
    private int cacheSize = 16;  // MB
    private int bulkSize = 10;  // items per batch

    private boolean bulkCommit = false;
    private boolean stableMode = false;
    // TTL for stable-mode buffer entries (minutes). 0 disables TTL handling.
    // default: 3 days = 60 * 24 * 3 minutes
    private long bufferTtlMinutes = 60L * 24L * 3L;

    public int getAutoCommitDelay() { return autoCommitDelay; }
    public void setAutoCommitDelay(int autoCommitDelay) { this.autoCommitDelay = autoCommitDelay; }

    public int getAutoCommitMemory() { return autoCommitMemory; }
    public void setAutoCommitMemory(int autoCommitMemory) { this.autoCommitMemory = autoCommitMemory; }

    public boolean isBulkCommit() { return bulkCommit; }
    public void setBulkCommit(boolean bulkCommit) { this.bulkCommit = bulkCommit; }

    public int getCacheSize() { return cacheSize; }
    public void setCacheSize(int cacheSize) { this.cacheSize = cacheSize; }

    public int getBulkSize() { return bulkSize; }
    public void setBulkSize(int bulkSize) { this.bulkSize = bulkSize; }

    public boolean isStableMode() { return stableMode; }
    public void setStableMode(boolean stableMode) { this.stableMode = stableMode; }

    public long getBufferTtlMinutes() { return bufferTtlMinutes; }
    public void setBufferTtlMinutes(long bufferTtlMinutes) { this.bufferTtlMinutes = bufferTtlMinutes; }
}
