package com.queue.file.vo;

import java.time.LocalDateTime;

/**
 * @since : 2025-07-07(월)
 */
public class DataSyncInfo {
    public DataSyncInfo() {
    }
    public DataSyncInfo(int dataCount, int keyCount, int syncCount, long maxKey) {
        this.dataCount = dataCount;
        this.keyCount = keyCount;
        this.syncCount = syncCount;
        this.maxKey = maxKey;
        this.lastSyncTime = LocalDateTime.now();
    }

    private int keyCount = 0;

    private int dataCount = 0;

    private int syncCount = 0;

    private long maxKey = 0;

    private LocalDateTime lastSyncTime;

    public int getKeyCount() { return keyCount; }
    public void setKeyCount(int keyCount) { this.keyCount = keyCount; }

    public int getDataCount() { return dataCount; }
    public void setDataCount(int dataCount) { this.dataCount = dataCount; }

    public int getSyncCount() { return syncCount; }
    public void setSyncCount(int syncCount) { this.syncCount = syncCount; }

    public long getMaxKey() { return maxKey; }
    public void setMaxKey(long maxKey) { this.maxKey = maxKey; }

    public LocalDateTime getLastSyncTime() { return lastSyncTime; }
    public void setLastSyncTime(LocalDateTime lastSyncTime) { this.lastSyncTime = lastSyncTime; }
}
