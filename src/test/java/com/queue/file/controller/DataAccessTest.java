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

public class DataAccessTest {

    @Test
    public void testWriteAndRead() throws Exception {
        Path file = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(file.getParent().toString(), file.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        PartitionManager pm = new PartitionManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, pm);

        String partition = "p1";
        String executor = "exec";
        da.write("tag", partition, executor, "data1");
        FileQueueData read = da.read(partition, executor);
        assertNotNull(read);
        assertEquals("data1", read.getData());
        da.readCommit(partition, executor);

        Files.deleteIfExists(file);
    }

    @Test
    public void testWriteBulkReadBulk() throws Exception {
        Path file = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(file.getParent().toString(), file.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        PartitionManager pm = new PartitionManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, pm);

        String partition = "p2";
        String executor = "exec";
        List<String> dataList = Arrays.asList("d1", "d2", "d3");
        da.writeBulk("tag", partition, executor, dataList);
        List<FileQueueData> readList = da.read(partition, executor, 3);
        assertNotNull(readList);
        assertEquals(3, readList.size());
        assertEquals("d1", readList.get(0).getData());
        da.readCommit(partition, executor);

        Files.deleteIfExists(file);
    }
}
