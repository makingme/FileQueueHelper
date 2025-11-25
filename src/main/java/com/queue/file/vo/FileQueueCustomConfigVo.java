package com.queue.file.vo;

public class FileQueueCustomConfigVo {
    private int autoCommitDelay = 1000;
    private int autoCommitMemory = 1024*19;
    private int cacheSize = 16;
    private int bulkSize = 10;

    private boolean bulkCommit = false;
    private boolean stableMode = false;

    // v2.1.0: 파티션별 독립 파일 모드 (기본값: false - 하위 호환성)
    private boolean useSeparatePartitionFiles = false;

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

    public boolean isUseSeparatePartitionFiles() { return useSeparatePartitionFiles; }
    public void setUseSeparatePartitionFiles(boolean useSeparatePartitionFiles) {
        this.useSeparatePartitionFiles = useSeparatePartitionFiles;
    }
}
