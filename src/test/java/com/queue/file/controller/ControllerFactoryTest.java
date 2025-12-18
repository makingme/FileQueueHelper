package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import com.queue.file.utils.Contents;
import com.queue.file.vo.FileQueueData;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class ControllerFactoryTest {

    /**
     * Recursively delete directory and all its contents
     */
    private void deleteDirectory(Path dir) throws IOException {
        if (Files.exists(dir)) {
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
            }
        }
    }

    @Test
    // 기본 모드 컨트롤러 생성 후 단일 IO 동작 검증
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
        deleteDirectory(dir);
    }

    @Test
    // Stable 모드에서 버퍼 격리/커밋 동작 검증
    public void testStableModeBuffer() throws Exception {
        Path dir = Files.createTempDirectory("stable");
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
        deleteDirectory(dir);
    }

    @Test(expected = InitializeException.class)
    // 존재하지 않는 경로 생성 시 InitializeException 발생 검증
    public void testCreateInvalidPath() {
        try(BaseController controller = ControllerFactory.create("/not/exist/path/queue.mv")){
            controller.getSummaryInfo();
        }
    }

    @Test
    // Bulk 설정 적용 여부 및 bulk read/write 검증
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
        deleteDirectory(dir);
    }

    @Test
    // checkStore 호출이 정상 동작하는지 검증
    public void testCheckStore() throws Exception {
        Path dir = Files.createTempDirectory("check");
        String queue = dir.resolve("c.mv").toString();

        BaseController controller = ControllerFactory.create(queue);
        controller.write("data");
        controller.read();
        controller.readCommit(Thread.currentThread().getName());
        controller.close();

        ControllerFactory.checkStore(queue);

        deleteDirectory(dir);
    }
}
