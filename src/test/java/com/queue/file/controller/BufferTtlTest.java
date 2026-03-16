package com.queue.file.controller;

import com.queue.file.utils.Contents;
import com.queue.file.vo.*;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BufferTtlTest {

    private Path tempFile;
    private MVStore store;

    @Before
    public void setUp() throws Exception {
        tempFile = TestSupport.createTempFile("queue-ttl", ".mv");
        store = new MVStore.Builder().open();
    }

    @After
    public void tearDown() throws Exception {
        if (store != null && !store.isClosed()) {
            store.close();
        }
        TestSupport.deleteIfExists(tempFile);
    }

    @Test
    // 테스트 목적: readShouldReturnDataEvenWhenBufferTtlExceeded 동작 확인
    public void readShouldReturnDataEvenWhenBufferTtlExceeded() throws Exception {
        FileQueueConfigVo config = new FileQueueConfigVo(tempFile.getParent().toString(), tempFile.getFileName().toString());
        FileQueueCustomConfigVo custom = new FileQueueCustomConfigVo();
        custom.setStableMode(true);
        custom.setBufferTtlMinutes(1);
        config.setCustomConfig(custom);

        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(store);
        storeInfo.setStoreOpenTime(LocalDateTime.now());

        QueueManager queueManager = new QueueManager(storeInfo);
        DataAccess dataAccess = new DataAccess(storeInfo, queueManager);
        MVMap<String, BufferEntry> bufferMap = queueManager.getQueueContext().getReadBufferMap();
        BufferEntry expired = new BufferEntry(
                Arrays.asList(new FileQueueData("tag-expired", "data-expired")),
                System.currentTimeMillis() - (2 * 60_000L)
        );
        bufferMap.put("consumer-1", expired);

        List<FileQueueData> result = dataAccess.read("consumer-1", 1);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("data-expired", result.get(0).getData());
    }

    @Test
    // 테스트 목적: initShouldCleanExpiredBufferEntries 동작 확인
    public void initShouldCleanExpiredBufferEntries() throws Exception {
        FileQueueConfigVo config = new FileQueueConfigVo(tempFile.getParent().toString(), tempFile.getFileName().toString());
        FileQueueCustomConfigVo custom = new FileQueueCustomConfigVo();
        custom.setStableMode(true);
        custom.setBufferTtlMinutes(1);
        config.setCustomConfig(custom);
        MVMap<String, BufferEntry> bufferMap = store.openMap(Contents.READ_BUFFER_NAME);
        BufferEntry expired = new BufferEntry(
                Arrays.asList(new FileQueueData("tag-expired", "data-expired")),
                System.currentTimeMillis() - (2 * 60_000L)
        );
        bufferMap.put("consumer-1", expired);

        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(store);
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        QueueManager queueManager = new QueueManager(storeInfo);
        MVMap<String, BufferEntry> cleanedMap = queueManager.getQueueContext().getReadBufferMap();

        assertTrue("Expired buffer entry should be cleaned on init", cleanedMap.isEmpty());
    }
}
