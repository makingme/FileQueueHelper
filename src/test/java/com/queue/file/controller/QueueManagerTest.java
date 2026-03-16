package com.queue.file.controller;

import com.queue.file.exception.UnsteadyStateException;
import com.queue.file.vo.StoreInfo;
import org.junit.Test;

import java.nio.file.Path;

public class QueueManagerTest {

    @Test(expected = UnsteadyStateException.class)
    // 테스트 목적: checkStateThrowsWhenStoreFileMissing 동작 확인
    public void checkStateThrowsWhenStoreFileMissing() throws Exception {
        Path file = TestSupport.createTempFile("queue-missing", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        TestSupport.deleteIfExists(file);

        QueueManager qm = new QueueManager(storeInfo);
        qm.checkState();
    }
}
