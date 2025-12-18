package com.queue.file.controller;

import com.queue.file.vo.*;
import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.Assert.*;

/**
 * v3.0.0: Updated for single queue (partition concept removed)
 */
public class BaseControllerTest {

    @Test
    // 단일 컨트롤러에서 write/read/commit 후 요약 정보가 맞는지 검증
    public void testWriteReadAndSummary() throws Exception {
        Path file = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(file.getParent().toString(), file.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());

        BaseController controller = new BaseController(storeInfo);

        // Write and read single data
        controller.write("data1");
        FileQueueData read = controller.read();
        assertNotNull(read);
        assertEquals("data1", read.getData());
        controller.readCommit(Thread.currentThread().getName());

        // Check data is empty after commit
        List<FileQueueData> dataList = controller.getDataList();
        assertTrue(dataList.isEmpty());

        // Write more data
        controller.write("data2");
        controller.write("data3");

        // Check summary
        QueueSummaryVo summary = controller.getSummaryInfo();
        assertEquals(2, summary.getDataCount());

        controller.close();
        assertTrue(storeInfo.getStore().isClosed());
        Files.deleteIfExists(file);
    }
}
