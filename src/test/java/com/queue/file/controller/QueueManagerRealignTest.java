package com.queue.file.controller;

import com.queue.file.vo.FileQueueData;
import com.queue.file.vo.QueueContext;
import com.queue.file.vo.StoreInfo;
import org.junit.Test;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueueManagerRealignTest {

    @Test
    // 테스트 목적: realignDataKeyIfNeededRunsOnInterval 동작 확인
    public void realignDataKeyIfNeededRunsOnInterval() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);

        QueueContext ctx = qm.getQueueContext();
        ctx.getDataMap().put(1L, new FileQueueData("tag", "data1"));
        ctx.getDataMap().put(2L, new FileQueueData("tag", "data2"));
        ctx.getTransactionKeyList().clear();

        for (int i = 0; i < 999; i++) {
            qm.realignDataKeyIfNeeded();
        }
        assertEquals(0, ctx.getTransactionKeyList().size());

        qm.realignDataKeyIfNeeded();
        assertEquals(2, ctx.getTransactionKeyList().size());
        assertTrue(ctx.getTransactionKeyList().contains(1L));
        assertTrue(ctx.getTransactionKeyList().contains(2L));

        TestSupport.deleteIfExists(file);
    }
}
