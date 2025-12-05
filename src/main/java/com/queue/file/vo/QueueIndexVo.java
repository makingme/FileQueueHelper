package com.queue.file.vo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * v2.1.0: Renamed from PartitionIndexVo (partition concept removed)
 * Manages transaction and group indices for single queue
 *
 * <p>Note: Removed MAX_VALUE wrapping logic as it's extremely unlikely to overflow
 * (would require billions of operations over many years)
 *
 * @since : 2025-11-28
 */
public class QueueIndexVo {
    private final AtomicLong transactionIndex = new AtomicLong(1);
    private final AtomicLong groupIndex = new AtomicLong(1);

    public Long getGroupIndex() {
        return groupIndex.incrementAndGet();
    }

    public Long getTransactionIndex() {
        return transactionIndex.incrementAndGet();
    }

    public void setTransactionIndex(long transactionIndex) {
        this.transactionIndex.set(transactionIndex);
    }
}
