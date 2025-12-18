package com.queue.file.controller;

import com.queue.file.vo.*;
import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;

import static org.junit.Assert.*;

/**
 * v3.0.0: Updated for single queue (partition concept removed)
 */
    public class StatsTrackerTest {

    @Test
    // 액션별 카운트가 누적/집계되는지 검증
    public void testKeepRecordCounts() {
        StatsTracker tracker = new StatsTracker();
        String exec = "EXEC";

        tracker.keepRecord(exec, 2, ActionType.INPUT);
        tracker.keepRecord(exec, 1, ActionType.OUTPUT);
        tracker.keepRecord(exec, 3, ActionType.BUFFER_INPUT);
        tracker.keepRecord(exec, 4, ActionType.BUFFER_OUTPUT);

        InOutStorage info = tracker.getInOutInfo();
        assertEquals(Long.valueOf(2), info.getInputCount());
        assertEquals(Long.valueOf(1), info.getOutputCount());
        assertEquals(Long.valueOf(3), info.getBufferInputCount());
        assertEquals(Long.valueOf(4), info.getBufferOutputCount());

        assertEquals(Long.valueOf(2), tracker.getTOTAL_INPUT_COUNT());
        assertEquals(Long.valueOf(1), tracker.getTOTAL_OUTPUT_COUNT());
    }

    @Test
    // DataAccess 간단 흐름에서 StatsTracker 연동 여부 검증
    public void testDataAccessSimpleFlow() throws Exception {
        Path dummyFile = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(dummyFile.getParent().toString(), dummyFile.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        String exec = "exec";
        da.write("tag", exec, "data1");
        FileQueueData data = da.read(exec);
        assertNotNull(data);
        assertEquals("data1", data.getData());
        da.readCommit(exec);

        Files.deleteIfExists(dummyFile);
    }
}

