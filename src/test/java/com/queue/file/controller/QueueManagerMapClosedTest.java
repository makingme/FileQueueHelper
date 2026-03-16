package com.queue.file.controller;

import com.queue.file.exception.UnsteadyStateException;
import com.queue.file.vo.StoreInfo;
import org.h2.mvstore.MVMap;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.file.Path;

public class QueueManagerMapClosedTest {

    @Test(expected = UnsteadyStateException.class)
    // 테스트 목적: checkStateThrowsWhenDataMapClosedBestEffort 동작 확인
    public void checkStateThrowsWhenDataMapClosedBestEffort() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);

        MVMap<Long, ?> dataMap = qm.getQueueContext().getDataMap();
        Method close = findCloseMethod(dataMap);
        // 환경 의존: MVMap.close는 H2 버전에 따라 없을 수 있어 미지원 시 스킵.
        Assume.assumeTrue("MVMap.close not available", close != null);
        close.invoke(dataMap);

        qm.checkState();
    }

    @Test(expected = UnsteadyStateException.class)
    // 테스트 목적: checkStateThrowsWhenReadBufferMapClosedBestEffort 동작 확인
    public void checkStateThrowsWhenReadBufferMapClosedBestEffort() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        QueueManager qm = new QueueManager(storeInfo);

        MVMap<String, ?> bufferMap = qm.getQueueContext().getReadBufferMap();
        Method close = findCloseMethod(bufferMap);
        // 환경 의존: MVMap.close는 H2 버전에 따라 없을 수 있어 미지원 시 스킵.
        Assume.assumeTrue("MVMap.close not available", close != null);
        close.invoke(bufferMap);

        qm.checkState();
    }

    private Method findCloseMethod(Object target) {
        if (target == null) {
            return null;
        }
        try {
            return target.getClass().getMethod("close");
        } catch (NoSuchMethodException e) {
            return null;
        }
    }
}
