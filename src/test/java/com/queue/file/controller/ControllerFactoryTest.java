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

        controller.getStoreInfo().getStore().close();
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

        controller.getStoreInfo().getStore().close();
        Files.deleteIfExists(dir.resolve("s.mv"));
        Files.deleteIfExists(dir);
    }

    @Test(expected = InitializeException.class)
    public void testCreateInvalidPath() {
        ControllerFactory.create("/not/exist/path/queue.mv");
    }
}
