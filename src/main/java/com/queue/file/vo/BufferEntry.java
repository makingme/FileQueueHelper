package com.queue.file.vo;

import java.io.Serializable;
import java.util.List;

/**
 * Wraps buffered data with last access time for TTL handling.
 */
public class BufferEntry implements Serializable {
    private final List<FileQueueData> data;
    private final long lastAccessEpochMillis;

    public BufferEntry(List<FileQueueData> data, long lastAccessEpochMillis) {
        this.data = data;
        this.lastAccessEpochMillis = lastAccessEpochMillis;
    }

    public List<FileQueueData> getData() {
        return data;
    }

    public long getLastAccessEpochMillis() {
        return lastAccessEpochMillis;
    }

    public BufferEntry touch() {
        return new BufferEntry(data, System.currentTimeMillis());
    }
}
