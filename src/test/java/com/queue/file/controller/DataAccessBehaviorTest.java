package com.queue.file.controller;

import com.queue.file.vo.BufferEntry;
import com.queue.file.vo.FileQueueCustomConfigVo;
import com.queue.file.vo.FileQueueData;
import com.queue.file.vo.StoreInfo;
import com.queue.file.utils.Contents;
import org.h2.mvstore.MVMap;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class DataAccessBehaviorTest {

    @Test
    // 테스트 목적: writeIgnoresNullOrEmpty 동작 확인
    public void writeIgnoresNullOrEmpty() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        da.write("tag", "exec", null);
        da.write("tag", "exec", "");

        assertEquals(0, qm.getQueueContext().getDataMap().size());
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: writeBulkIgnoresNullOrEmptyList 동작 확인
    public void writeBulkIgnoresNullOrEmptyList() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        da.writeBulk("tag", "exec", null);
        da.writeBulk("tag", "exec", Collections.<String>emptyList());

        assertEquals(0, qm.getQueueContext().getDataMap().size());
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: readReturnsNullWhenRequestCountNonPositive 동작 확인
    public void readReturnsNullWhenRequestCountNonPositive() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        assertNull(da.read("consumer", 0));
        assertNull(da.read("consumer", -1));
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: readReturnsNullWhenDataMapEmpty 동작 확인
    public void readReturnsNullWhenDataMapEmpty() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        assertNull(da.read("consumer"));
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: readCommitNoBufferNoException 동작 확인
    public void readCommitNoBufferNoException() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        da.readCommit("consumer");
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: stableModeReadsBufferFirstAndTouchesEntry 동작 확인
    public void stableModeReadsBufferFirstAndTouchesEntry() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        FileQueueCustomConfigVo custom = new FileQueueCustomConfigVo();
        custom.setStableMode(true);
        StoreInfo storeInfo = TestSupport.newStoreInfo(file, custom);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        long oldTs = System.currentTimeMillis() - 60_000L;
        List<FileQueueData> buffered = Arrays.asList(new FileQueueData("tag", "data"));
        MVMap<String, BufferEntry> bufferMap = qm.getQueueContext().getReadBufferMap();
        bufferMap.put("consumer", new BufferEntry(buffered, oldTs));

        List<FileQueueData> result = da.read("consumer", 1);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("data", result.get(0).getData());

        BufferEntry updated = bufferMap.get("consumer");
        assertNotNull(updated);
        assertTrue(updated.getLastAccessEpochMillis() > oldTs);

        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: readCommitRemovesBufferEntry 동작 확인
    public void readCommitRemovesBufferEntry() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        FileQueueCustomConfigVo custom = new FileQueueCustomConfigVo();
        custom.setStableMode(true);
        StoreInfo storeInfo = TestSupport.newStoreInfo(file, custom);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        da.write("tag", "exec", "data");
        List<FileQueueData> read = da.read("consumer", 1);
        assertNotNull(read);
        assertTrue(qm.getQueueContext().getReadBufferMap().containsKey("consumer"));

        da.readCommit("consumer");
        assertFalse(qm.getQueueContext().getReadBufferMap().containsKey("consumer"));
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: removeOnePrefersBufferEntry 동작 확인
    public void removeOnePrefersBufferEntry() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        da.write("tag", "exec", "data1");
        MVMap<String, BufferEntry> bufferMap = storeInfo.getStore().openMap(Contents.READ_BUFFER_NAME);
        bufferMap.put("consumer", new BufferEntry(Arrays.asList(new FileQueueData("tag", "buf")), System.currentTimeMillis()));

        da.removeOne("consumer");

        assertFalse(bufferMap.containsKey("consumer"));
        assertEquals(1, qm.getQueueContext().getDataMap().size());
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: getSummaryInfoCountsCacheEntries 동작 확인
    public void getSummaryInfoCountsCacheEntries() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        MVMap<String, FileQueueData> cacheMap = storeInfo.getStore().openMap(Contents.CACHE_NAME);
        cacheMap.put("k", new FileQueueData("tag", "data"));

        assertEquals(1, da.getSummaryInfo().getCacheCount());
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: cacheAccessAndClear 동작 확인
    public void cacheAccessAndClear() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        MVMap<String, FileQueueData> cacheMap = storeInfo.getStore().openMap(Contents.CACHE_NAME);
        cacheMap.put("k1", new FileQueueData("tag", "v1"));
        cacheMap.put("k2", new FileQueueData("tag", "v2"));

        assertNotNull(da.getCache("k1"));
        assertEquals(2, da.getCacheList().size());

        da.clearCache("k1");
        assertNull(da.getCache("k1"));
        assertNotNull(da.getCache("k2"));

        da.clearCache();
        assertEquals(0, da.getCacheList().size());
        TestSupport.deleteIfExists(file);
    }
}
