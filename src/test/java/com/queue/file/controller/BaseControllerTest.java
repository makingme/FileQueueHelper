package com.queue.file.controller;

import com.queue.file.vo.FileQueueData;
import com.queue.file.vo.QueueSummaryVo;
import com.queue.file.vo.StoreInfo;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.*;

/**
 * v3.0.0: Updated for single queue (partition concept removed)
 */
public class BaseControllerTest {

    @Test
    // 테스트 목적: testWriteReadAndSummary 동작 확인
    public void testWriteReadAndSummary() throws Exception {
        Path file = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(file);

        BaseController controller = new BaseController(storeInfo);

        // 데이터 단건 쓰기/읽기
        controller.write("data1");
        FileQueueData read = controller.read();
        assertNotNull(read);
        assertEquals("data1", read.getData());
        controller.readCommit(Thread.currentThread().getName());

        // 커밋 후 데이터 비움 확인
        List<FileQueueData> dataList = controller.getDataList();
        assertTrue(dataList.isEmpty());

        // 추가 데이터 쓰기
        controller.write("data2");
        controller.write("data3");

        // 요약 정보 확인
        QueueSummaryVo summary = controller.getSummaryInfo();
        assertEquals(2, summary.getDataCount());

        controller.close();
        assertTrue(storeInfo.getStore().isClosed());
        TestSupport.deleteIfExists(file);
    }
}
