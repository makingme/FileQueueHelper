package com.queue.file.controller;

import com.queue.file.vo.*;
import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * v3.0.0: Updated for single queue (partition concept removed)
 */
    public class DataAccessTest {

    @Test
    // 단일 write 후 read/commit 흐름 검증
    public void testWriteAndRead() throws Exception {
        Path file = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(file.getParent().toString(), file.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        String executor = "exec";
        da.write("tag", executor, "data1");
        FileQueueData read = da.read(executor);
        assertNotNull(read);
        assertEquals("data1", read.getData());
        da.readCommit(executor);

        Files.deleteIfExists(file);
    }

    @Test
    // bulk write 후 다건 read/commit 흐름 검증
    public void testWriteBulkReadBulk() throws Exception {
        Path file = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(file.getParent().toString(), file.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        String executor = "exec";
        List<String> dataList = Arrays.asList("d1", "d2", "d3");
        da.writeBulk("tag", executor, dataList);
        List<FileQueueData> readList = da.read(executor, 3);
        assertNotNull(readList);
        assertEquals(3, readList.size());
        assertEquals("d1", readList.get(0).getData());
        da.readCommit(executor);

        Files.deleteIfExists(file);
    }
}
