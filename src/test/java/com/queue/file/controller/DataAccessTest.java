package com.queue.file.controller;

import com.queue.file.exception.QueueException;
import com.queue.file.exception.QueueReadException;
import com.queue.file.exception.UnsteadyStateException;
import com.queue.file.vo.FileQueueData;
import com.queue.file.vo.QueueContext;
import com.queue.file.vo.StoreInfo;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * v3.0.0: Updated for single queue (partition concept removed)
 */
public class DataAccessTest {

    @Test
    // 테스트 목적: testWriteAndRead 동작 확인
    public void testWriteAndRead() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        String executor = "exec";
        da.write("tag", executor, "data1");
        FileQueueData read = da.read(executor);
        assertNotNull(read);
        assertEquals("data1", read.getData());
        da.readCommit(executor);

        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: testWriteBulkReadBulk 동작 확인
    public void testWriteBulkReadBulk() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
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

        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: readShouldKeepDataAndKeyListInSync 동작 확인
    public void readShouldKeepDataAndKeyListInSync() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        da.writeBulk("tag", "exec", Arrays.asList("d1", "d2", "d3"));
        List<FileQueueData> readList = da.read("consumer", 2);
        assertNotNull(readList);
        assertEquals(2, readList.size());

        QueueContext ctx = qm.getQueueContext();
        assertEquals(1, ctx.getDataMap().size());
        assertEquals(1, ctx.getTransactionKeyList().size());
        Long remainingKey = ctx.getTransactionKeyList().first();
        assertNotNull(ctx.getDataMap().get(remainingKey));

        TestSupport.deleteIfExists(file);
    }

    @Test(expected = QueueReadException.class)
    // 테스트 목적: readCommitWithoutOpenTimeThrowsQueueReadException 동작 확인
    public void readCommitWithoutOpenTimeThrowsQueueReadException() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        storeInfo.setStoreOpenTime(null);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        da.readCommit("consumer");
    }

    @Test
    // 테스트 목적: writeShouldFailWhenStoreFileMissing 동작 확인
    public void writeShouldFailWhenStoreFileMissing() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        TestSupport.deleteIfExists(file);

        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        try {
            da.write("tag", "exec", "data");
            fail("Expected QueueException");
        } catch (QueueException e) {
            assertTrue(e.getCause() instanceof UnsteadyStateException);
            assertTrue(e.getCause().getMessage().contains("store file missing"));
        }
    }
}

