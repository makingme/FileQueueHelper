package com.queue.file.controller;

import com.queue.file.utils.Contents;
import com.queue.file.vo.BufferEntry;
import com.queue.file.vo.FileQueueData;
import com.queue.file.vo.StoreInfo;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class BaseControllerManagementTest {

    @Test
    // 테스트 목적: manageDataBufferAndCache 동작 확인
    public void manageDataBufferAndCache() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        storeInfo.setCacheStore(new MVStore.Builder().open());
        BaseController controller = new BaseController(storeInfo);

        controller.write("data1");
        controller.write("data2");
        List<FileQueueData> dataList = controller.getDataList();
        assertEquals(2, dataList.size());

        Long key = dataList.get(0).getTransactionKey();
        controller.removeData(key);
        assertEquals(1, controller.getDataList().size());

        controller.clearData();
        assertTrue(controller.getDataList().isEmpty());

        MVMap<String, BufferEntry> bufferMap = storeInfo.getStore().openMap(Contents.READ_BUFFER_NAME);
        bufferMap.put("consumer", new BufferEntry(Arrays.asList(new FileQueueData("tag", "buf")), System.currentTimeMillis()));
        controller.clearBuffer("consumer");
        assertFalse(bufferMap.containsKey("consumer"));

        bufferMap.put("consumer2", new BufferEntry(Arrays.asList(new FileQueueData("tag", "buf2")), System.currentTimeMillis()));
        controller.clearBuffer();
        assertTrue(bufferMap.isEmpty());

        MVMap<String, FileQueueData> cacheMap = storeInfo.getStore().openMap(Contents.CACHE_NAME);
        cacheMap.put("k", new FileQueueData("tag", "cache"));
        controller.clearCache();
        assertTrue(cacheMap.isEmpty());

        controller.close();
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: removeOneRemovesDataWhenNoBuffer 동작 확인
    public void removeOneRemovesDataWhenNoBuffer() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        BaseController controller = new BaseController(storeInfo);

        controller.write("d1");
        controller.write("d2");
        assertEquals(2, controller.getSummaryInfo().getDataCount());

        controller.removeOne("consumer");
        assertEquals(1, controller.getSummaryInfo().getDataCount());

        controller.close();
        TestSupport.deleteIfExists(file);
    }

    @Test
    // 테스트 목적: cacheStoreHelpersReturnMap 동작 확인
    public void cacheStoreHelpersReturnMap() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        storeInfo.setCacheStore(new MVStore.Builder().open());
        BaseController controller = new BaseController(storeInfo);

        assertNotNull(controller.getCacheStore());
        MVMap<String, String> map = controller.openCacheMap("CACHE_TEST");
        map.put("k", "v");
        assertEquals("v", map.get("k"));

        controller.close();
        TestSupport.deleteIfExists(file);
    }
}
