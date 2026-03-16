package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import com.queue.file.exception.QueueReadException;
import com.queue.file.vo.FileQueueConfigVo;
import com.queue.file.vo.StoreInfo;
import org.junit.Test;

import java.nio.file.Path;
import java.time.LocalDateTime;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Exception propagation and message templates.
 */
public class ExceptionHandlingTest {

    @Test
    // 테스트 목적: readWithoutOpenTimeThrowsQueueReadException 동작 확인
    public void readWithoutOpenTimeThrowsQueueReadException() throws Exception {
        Path file = TestSupport.createTempFile("queue-ex", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        storeInfo.setStoreOpenTime(null); // 미오픈 상태 시뮬레이션

        try {
            QueueManager qm = new QueueManager(storeInfo);
            DataAccess da = new DataAccess(storeInfo, qm);
            da.read("consumer");
            fail("Expected QueueReadException");
        } catch (QueueReadException e) {
            assertTrue(e.getMessage().contains("store not opened"));
        } finally {
            TestSupport.deleteIfExists(file);
        }
    }

    @Test
    // 테스트 목적: readWithNullDataMapThrowsQueueReadException 동작 확인
    public void readWithNullDataMapThrowsQueueReadException() throws Exception {
        Path file = TestSupport.createTempFile("queue-ex2", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        QueueManager qm = new QueueManager(storeInfo);
        qm.getQueueContext().setDataMap(null);

        try {
            DataAccess da = new DataAccess(storeInfo, qm);
            da.read("consumer");
            fail("Expected QueueReadException");
        } catch (QueueReadException e) {
            assertTrue(e.getMessage().contains("data map is closed or null"));
        } finally {
            TestSupport.deleteIfExists(file);
        }
    }

    @Test
    // 테스트 목적: queueManagerInitWithNullStoreThrowsInitializeException 동작 확인
    public void queueManagerInitWithNullStoreThrowsInitializeException() {
        FileQueueConfigVo config = new FileQueueConfigVo("path", "name");
        StoreInfo storeInfo = new StoreInfo(config);
        try {
            new QueueManager(storeInfo);
            fail("Expected InitializeException");
        } catch (InitializeException e) {
            assertTrue(e.getMessage().contains("store is null or closed"));
        }
    }

    @Test
    // 테스트 목적: readWithClosedStoreThrowsQueueReadException 동작 확인
    public void readWithClosedStoreThrowsQueueReadException() throws Exception {
        Path file = TestSupport.createTempFile("queue-ex3", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);
        storeInfo.getStore().close();

        try {
            da.read("consumer");
            fail("Expected QueueReadException");
        } catch (QueueReadException e) {
            assertTrue(e.getMessage().contains("data map is closed or null"));
        } finally {
            TestSupport.deleteIfExists(file);
        }
    }

    @Test
    // 테스트 목적: checkStateWithNullBufferMapThrowsUnsteadyStateException 동작 확인
    public void checkStateWithNullBufferMapThrowsUnsteadyStateException() throws Exception {
        Path file = TestSupport.createTempFile("queue-ex4", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        QueueManager qm = new QueueManager(storeInfo);
        qm.getQueueContext().setReadBufferMap(null);

        try {
            qm.checkState();
            fail("Expected UnsteadyStateException");
        } catch (com.queue.file.exception.UnsteadyStateException e) {
            assertTrue(e.getMessage().contains("read buffer map is invalid or closed"));
        } finally {
            TestSupport.deleteIfExists(file);
        }
    }

    @Test
    // 테스트 목적: controllerFactoryInvalidPathIncludesPathInMessage 동작 확인
    public void controllerFactoryInvalidPathIncludesPathInMessage() {
        String queue = java.nio.file.Paths.get("Z", "not", "exist", "queue.mv").toString();
        try {
            ControllerFactory.create(queue);
            fail("Expected InitializeException");
        } catch (InitializeException e) {
            assertTrue(e.getMessage().contains("queue path does not exist"));
        }
    }
}
