package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import com.queue.file.exception.QueueReadException;
import com.queue.file.exception.QueueWriteException;
import com.queue.file.vo.FileQueueData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * v2.1.0: 파티션별 독립 파일 모드 테스트
 */
public class SeparatePartitionFilesTest {
    private static final Logger logger = LoggerFactory.getLogger(SeparatePartitionFilesTest.class);
    private BaseController controller;
    private String testQueuePath;
    private String testQueueName = "testqueue";

    @Before
    public void setUp() throws Exception {
        // 테스트용 임시 디렉토리 생성
        Path tempDir = Files.createTempDirectory("filequeue-test");
        testQueuePath = tempDir.toString();
        logger.info("Test queue path: {}", testQueuePath);

        // 파티션별 독립 파일 모드로 컨트롤러 생성
        controller = ControllerFactory.createWithSeparatePartitionFiles(testQueuePath, testQueueName);
        assertNotNull(controller);
    }

    @After
    public void tearDown() throws Exception {
        if (controller != null) {
            controller.close();
        }

        // 테스트 파일 정리
        deleteTestFiles();
    }

    private void deleteTestFiles() {
        File dir = new File(testQueuePath);
        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.getName().startsWith(testQueueName)) {
                        file.delete();
                        logger.debug("Deleted test file: {}", file.getName());
                    }
                }
            }
        }
    }

    @Test
    public void testSeparatePartitionFilesCreated() throws QueueWriteException {
        logger.info("=== Test: 파티션별 독립 파일 생성 확인 ===");

        // 3개의 다른 파티션에 데이터 쓰기
        controller.write("partition1", "data1");
        controller.write("partition2", "data2");
        controller.write("partition3", "data3");

        // 파일 생성 확인
        File dir = new File(testQueuePath);
        File[] files = dir.listFiles((d, name) -> name.startsWith(testQueueName) && name.endsWith(".mv"));

        assertNotNull("파일 목록이 null입니다", files);
        assertTrue("최소 3개의 파티션 파일이 생성되어야 합니다", files.length >= 3);

        for (File file : files) {
            logger.info("Created partition file: {}", file.getName());
        }
    }

    @Test
    public void testWriteAndReadFromDifferentPartitions() throws QueueWriteException, QueueReadException {
        logger.info("=== Test: 다른 파티션에 쓰기/읽기 ===");

        String partition1 = "order";
        String partition2 = "payment";
        String executor = "test-executor";

        // 각 파티션에 데이터 쓰기
        controller.write(partition1, "order-data-1");
        controller.write(partition1, "order-data-2");
        controller.write(partition2, "payment-data-1");
        controller.write(partition2, "payment-data-2");

        // 각 파티션에서 독립적으로 읽기
        List<FileQueueData> orderData = controller.read(partition1, executor, 10);
        assertNotNull("order 파티션 데이터가 null입니다", orderData);
        assertEquals("order 파티션에 2개의 데이터가 있어야 합니다", 2, orderData.size());

        List<FileQueueData> paymentData = controller.read(partition2, executor, 10);
        assertNotNull("payment 파티션 데이터가 null입니다", paymentData);
        assertEquals("payment 파티션에 2개의 데이터가 있어야 합니다", 2, paymentData.size());

        // 데이터 내용 확인
        assertTrue("order 데이터 확인", orderData.get(0).getData().contains("order"));
        assertTrue("payment 데이터 확인", paymentData.get(0).getData().contains("payment"));

        logger.info("Order data: {}", orderData);
        logger.info("Payment data: {}", paymentData);
    }

    @Test
    public void testBulkWriteToMultiplePartitions() throws QueueWriteException {
        logger.info("=== Test: 여러 파티션에 대량 쓰기 ===");

        int dataCountPerPartition = 100;
        String[] partitions = {"p1", "p2", "p3"};

        for (String partition : partitions) {
            List<String> dataList = new ArrayList<>();
            for (int i = 0; i < dataCountPerPartition; i++) {
                dataList.add(partition + "-data-" + i);
            }
            controller.writeBulk(partition, dataList);
            logger.info("Written {} items to partition {}", dataCountPerPartition, partition);
        }

        // 파티션별 파일이 독립적으로 생성되었는지 확인
        File dir = new File(testQueuePath);
        File[] files = dir.listFiles((d, name) -> name.startsWith(testQueueName) && name.endsWith(".mv"));

        assertNotNull(files);
        assertTrue("파티션 파일들이 생성되어야 합니다", files.length >= partitions.length);
    }

    @Test
    public void testConcurrentAccessToDifferentPartitions() throws Exception {
        logger.info("=== Test: 다른 파티션에 동시 접근 ===");

        Thread writer1 = new Thread(() -> {
            try {
                for (int i = 0; i < 50; i++) {
                    controller.write("concurrent1", "data-" + i);
                }
                logger.info("Writer1 completed");
            } catch (Exception e) {
                logger.error("Writer1 error", e);
            }
        });

        Thread writer2 = new Thread(() -> {
            try {
                for (int i = 0; i < 50; i++) {
                    controller.write("concurrent2", "data-" + i);
                }
                logger.info("Writer2 completed");
            } catch (Exception e) {
                logger.error("Writer2 error", e);
            }
        });

        long startTime = System.currentTimeMillis();
        writer1.start();
        writer2.start();
        writer1.join();
        writer2.join();
        long elapsedTime = System.currentTimeMillis() - startTime;

        logger.info("Concurrent write completed in {} ms", elapsedTime);

        // 각 파티션에 50개씩 데이터가 있는지 확인
        List<FileQueueData> data1 = controller.read("concurrent1", "executor1", 100);
        List<FileQueueData> data2 = controller.read("concurrent2", "executor2", 100);

        assertNotNull(data1);
        assertNotNull(data2);
        assertEquals("concurrent1 파티션에 50개 데이터", 50, data1.size());
        assertEquals("concurrent2 파티션에 50개 데이터", 50, data2.size());
    }

    @Test
    public void testDefaultPartition() throws QueueWriteException, QueueReadException {
        logger.info("=== Test: 기본 파티션 동작 확인 ===");

        // 파티션 지정 없이 쓰기 (기본 파티션 사용)
        controller.write("default-data-1");
        controller.write("default-data-2");

        // 기본 파티션에서 읽기
        List<FileQueueData> data = controller.read("executor", 10);
        assertNotNull("기본 파티션 데이터가 null입니다", data);
        assertEquals("기본 파티션에 2개 데이터", 2, data.size());

        logger.info("Default partition data: {}", data);
    }
}
