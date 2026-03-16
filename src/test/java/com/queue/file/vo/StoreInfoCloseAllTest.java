package com.queue.file.vo;

import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertTrue;

public class StoreInfoCloseAllTest {

    @Test
    // 테스트 목적: closeAllClosesBothStores 동작 확인
    public void closeAllClosesBothStores() throws Exception {
        Path file = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(file.getParent().toString(), file.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setCacheStore(new MVStore.Builder().open());

        storeInfo.closeAll();

        assertTrue(storeInfo.getStore().isClosed());
        assertTrue(storeInfo.getCacheStore().isClosed());
        Files.deleteIfExists(file);
    }
}
