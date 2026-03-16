package com.queue.file.controller;

import com.queue.file.vo.ActionType;
import com.queue.file.vo.FileQueueData;
import com.queue.file.vo.InOutStorage;
import com.queue.file.vo.StoreInfo;
import org.junit.Test;

import java.nio.file.Path;

import static org.junit.Assert.*;

/**
 * v3.0.0: Updated for single queue (partition concept removed)
 */
public class StatsTrackerTest {

    @Test
    // 테스트 목적: testKeepRecordCounts 동작 확인
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
    // 테스트 목적: testDataAccessSimpleFlow 동작 확인
    public void testDataAccessSimpleFlow() throws Exception {
        Path dummyFile = TestSupport.createTempFile("queue", ".mv");
        StoreInfo storeInfo = TestSupport.newStoreInfo(dummyFile);
        QueueManager qm = new QueueManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, qm);

        String exec = "exec";
        da.write("tag", exec, "data1");
        FileQueueData data = da.read(exec);
        assertNotNull(data);
        assertEquals("data1", data.getData());
        da.readCommit(exec);

        TestSupport.deleteIfExists(dummyFile);
    }
}

