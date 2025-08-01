package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import com.queue.file.utils.Contents;
import com.queue.file.vo.FileQueueData;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;

import static org.junit.Assert.*;

public class ControllerFactoryTest {

    @Test
    public void testCreateAndBasicIO() throws Exception {
        Path dir = Files.createTempDirectory("qtest");
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
        Files.deleteIfExists(dir.resolve("t.mv"));
        Files.deleteIfExists(dir);
    }

    @Test
    public void testStableModeBuffer() throws Exception {
        Path dir = Files.createTempDirectory("stable");
        String queue = dir.resolve("s.mv").toString();

        BaseController controller = ControllerFactory.createStable(queue);
        controller.write("hello");

        FileQueueData data = controller.read();
        assertNotNull(data);
        Map<String, List<FileQueueData>> buffers = controller.getPartitionBufferList(Contents.DEFAULT_PARTITION);
        assertTrue(buffers.containsKey(Thread.currentThread().getName()));
        controller.readCommit(Thread.currentThread().getName());
        buffers = controller.getPartitionBufferList(Contents.DEFAULT_PARTITION);
        assertFalse(buffers.containsKey(Thread.currentThread().getName()));

        controller.close();
        assertTrue(controller.getStoreInfo().getStore().isClosed());
        Files.deleteIfExists(dir.resolve("s.mv"));
        Files.deleteIfExists(dir);
    }

    @Test(expected = InitializeException.class)
    public void testCreateInvalidPath() {
        try(BaseController controller = ControllerFactory.create("/not/exist/path/queue.mv")){
            controller.getSummaryInfo();
        }
    }

    @Test
    public void testBulkCommitConfig() throws Exception {
        Path dir = Files.createTempDirectory("bulk");
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
        Files.deleteIfExists(dir.resolve("b.mv"));
        Files.deleteIfExists(dir);
    }

    @Test
    public void testCheckStore() throws Exception {
        Path dir = Files.createTempDirectory("check");
        String queue = dir.resolve("c.mv").toString();

        BaseController controller = ControllerFactory.create(queue);
        controller.write("data");
        controller.read();
        controller.readCommit(Thread.currentThread().getName());
        controller.close();

        ControllerFactory.checkStore(queue);

        Files.deleteIfExists(dir.resolve("c.mv"));
        Files.deleteIfExists(dir);
    }
}
