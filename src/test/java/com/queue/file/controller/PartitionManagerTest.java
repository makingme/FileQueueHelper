package com.queue.file.controller;

import com.queue.file.utils.Contents;
import com.queue.file.vo.*;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.Assert.*;

public class PartitionManagerTest {

    @Test
    public void testRealignDataKey() throws Exception {
        Path file = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(file.getParent().toString(), file.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        PartitionManager pm = new PartitionManager(storeInfo);

        PartitionContext ctx = pm.getPartitionContext("P1");
        MVMap<Long, FileQueueData> map = ctx.getDataMap();
        map.put(1L, new FileQueueData("P1", "t", "d1"));
        map.put(2L, new FileQueueData("P1", "t", "d2"));

        assertTrue(ctx.getTransactionKeyList().isEmpty());
        pm.realignDataKey("P1");
        assertEquals(2, ctx.getTransactionKeyList().size());
        assertTrue(ctx.getTransactionKeyList().containsAll(Arrays.asList(1L,2L)));
        assertNotNull(ctx.getSyncInfo());

        Files.deleteIfExists(file);
    }
}
