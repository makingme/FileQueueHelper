package com.queue.file.vo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @since : 2025-07-16(수)
 */
public class PartitionIndexVo {
    private final AtomicLong transactionIndex = new AtomicLong(1);
    private final AtomicLong groupIndex = new AtomicLong(1);

    public Long getGroupIndex() {
        groupIndex.compareAndSet(Long.MAX_VALUE - 1, 1L);
        return groupIndex.incrementAndGet();
    }

    public Long getTransactionIndex() {
        transactionIndex.compareAndSet(Long.MAX_VALUE - 1, 1L);
        return transactionIndex.incrementAndGet();
    }

    public void setTransactionIndex(long transactionIndex) {
        this.transactionIndex.set(transactionIndex);
    }
}
