package com.queue.file.controller;

import com.queue.file.utils.Contents;
import com.queue.file.vo.*;
import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class BaseControllerTest {

    @Test
    public void testWriteReadAndSummary() throws Exception {
        Path file = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(file.getParent().toString(), file.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());

        BaseController controller = new BaseController(storeInfo);

        controller.write("data1");
        FileQueueData read = controller.read();
        assertNotNull(read);
        assertEquals("data1", read.getData());
        controller.readCommit(Thread.currentThread().getName());

        Map<String, List<FileQueueData>> allData = controller.getAllDataList();
        assertTrue(allData.get(Contents.DEFAULT_PARTITION).isEmpty());

        controller.write("P1", "data2");
        Set<String> names = controller.getAllPartitionNameSet();
        assertTrue(names.contains("P1"));

        Map<String, PartitionSummaryVo> summary = controller.getSummaryInfo();
        assertEquals(1, summary.get("P1").getDataCount());

        storeInfo.getStore().close();
        Files.deleteIfExists(file);
    }
}
