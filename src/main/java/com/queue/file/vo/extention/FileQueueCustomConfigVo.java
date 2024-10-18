package com.queue.file.vo.extention;

public class FileQueueCustomConfigVo {
    private int autoCommitDelay = 1000;
    private int autoCommitMemory = 1024*19;
    private int cacheSize = 16;
    private int bulkSize = 10;

    private long waitTime = 2000;
    private boolean bulkCommit = false;
    private boolean manualCommitMode = false;

    public int getAutoCommitDelay() { return autoCommitDelay; }
    public void setAutoCommitDelay(int autoCommitDelay) { this.autoCommitDelay = autoCommitDelay; }

    public int getAutoCommitMemory() { return autoCommitMemory; }
    public void setAutoCommitMemory(int autoCommitMemory) { this.autoCommitMemory = autoCommitMemory; }

    public long getWaitTime() { return waitTime; }
    public void setWaitTime(long waitTime) { this.waitTime = waitTime; }

    public boolean isBulkCommit() { return bulkCommit; }
    public void setBulkCommit(boolean bulkCommit) { this.bulkCommit = bulkCommit; }

    public int getCacheSize() { return cacheSize; }
    public void setCacheSize(int cacheSize) { this.cacheSize = cacheSize; }

    public int getBulkSize() { return bulkSize; }
    public void setBulkSize(int bulkSize) { this.bulkSize = bulkSize; }

    public boolean isManualCommitMode() { return manualCommitMode; }
    public void setManualCommitMode(boolean manualCommitMode) { this.manualCommitMode = manualCommitMode; }

    public void setStableMode(){
        manualCommitMode = true;
        autoCommitDelay = 0;
    }

    public void setBulkCommitMode(){
        bulkCommit = true;
    }

}
