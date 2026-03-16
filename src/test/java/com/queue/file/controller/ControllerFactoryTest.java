package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import com.queue.file.vo.FileQueueData;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ControllerFactoryTest {

    @Test
    // 테스트 목적: testCreateAndBasicIO 동작 확인
    public void testCreateAndBasicIO() throws Exception {
        Path dir = TestSupport.createTempDirectory("qtest");
        String queue = dir.resolve("t.mv").toString();

        BaseController controller = ControllerFactory.create(queue);
        assertNotNull(controller);

        controller.write("hello");
        FileQueueData data = controller.read();
        assertNotNull(data);
        assertEquals("hello", data.getData());
        controller.readCommit(Thread.currentThread().getName());

        controller.close();
        assertTrue(controller.getStoreInfo().getStore().isClosed());
        TestSupport.deleteDirectory(dir);
    }

    @Test(expected = InitializeException.class)
    // 테스트 목적: testCreateWithEmptyQueueString 동작 확인
    public void testCreateWithEmptyQueueString() {
        ControllerFactory.create("");
    }

    @Test
    // 테스트 목적: testStableModeBuffer 동작 확인
    public void testStableModeBuffer() throws Exception {
        Path dir = TestSupport.createTempDirectory("stable");
        String queue = dir.resolve("s.mv").toString();

        BaseController controller = ControllerFactory.createStable(queue);
        controller.write("hello");

        FileQueueData data = controller.read();
        assertNotNull(data);
        Map<String, List<FileQueueData>> buffers = controller.getBufferList();
        assertTrue(buffers.containsKey(Thread.currentThread().getName()));
        controller.readCommit(Thread.currentThread().getName());
        buffers = controller.getBufferList();
        assertFalse(buffers.containsKey(Thread.currentThread().getName()));

        controller.close();
        assertTrue(controller.getStoreInfo().getStore().isClosed());
        TestSupport.deleteDirectory(dir);
    }

    @Test(expected = InitializeException.class)
    // 테스트 목적: testCreateInvalidPath 동작 확인
    public void testCreateInvalidPath() {
        try(BaseController controller = ControllerFactory.create("/not/exist/path/queue.mv")){
            controller.getSummaryInfo();
        }
    }

    @Test
    // 테스트 목적: testBulkCommitConfig 동작 확인
    public void testBulkCommitConfig() throws Exception {
        Path dir = TestSupport.createTempDirectory("bulk");
        String queue = dir.resolve("b.mv").toString();

        BaseController controller = ControllerFactory.createBulk(queue, 2);
        assertNotNull(controller);
        assertTrue(controller.getStoreInfo().getCONFIG().getCustomConfig().isBulkCommit());
        assertEquals(2, controller.getStoreInfo().getCONFIG().getCustomConfig().getBulkSize());

        controller.writeBulk(java.util.Arrays.asList("d1", "d2"));

        java.util.List<FileQueueData> datas = controller.read(Thread.currentThread().getName(), 2);
        assertEquals(2, datas.size());
        assertEquals("d1", datas.get(0).getData());
        assertEquals("d2", datas.get(1).getData());
        controller.readCommit(Thread.currentThread().getName());

        controller.close();
        TestSupport.deleteDirectory(dir);
    }

    @Test
    // 테스트 목적: testCheckStore 동작 확인
    public void testCheckStore() throws Exception {
        Path dir = TestSupport.createTempDirectory("check");
        String queue = dir.resolve("c.mv").toString();

        BaseController controller = ControllerFactory.create(queue);
        controller.write("data");
        controller.read();
        controller.readCommit(Thread.currentThread().getName());
        controller.close();

        ControllerFactory.checkStore(queue);

        TestSupport.deleteDirectory(dir);
    }
}
