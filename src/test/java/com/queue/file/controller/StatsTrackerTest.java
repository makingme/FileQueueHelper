package com.queue.file.controller;

import com.queue.file.vo.*;
import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;

import static org.junit.Assert.*;

public class StatsTrackerTest {

    @Test
    public void testKeepRecordCounts() {
        StatsTracker tracker = new StatsTracker();
        String partition = "P1";
        String exec = "EXEC";

        tracker.keepRecord(partition, exec, 2, ActionType.INPUT);
        tracker.keepRecord(partition, exec, 1, ActionType.OUTPUT);
        tracker.keepRecord(partition, exec, 3, ActionType.BUFFER_INPUT);
        tracker.keepRecord(partition, exec, 4, ActionType.BUFFER_OUTPUT);

        InOutStorage info = tracker.getInOutInfo(partition);
        assertEquals(Long.valueOf(2), info.getInputCount());
        assertEquals(Long.valueOf(1), info.getOutputCount());
        assertEquals(Long.valueOf(3), info.getBufferInputCount());
        assertEquals(Long.valueOf(4), info.getBufferOutputCount());

        assertEquals(Long.valueOf(2), tracker.getTOTAL_INPUT_COUNT());
        assertEquals(Long.valueOf(1), tracker.getTOTAL_OUTPUT_COUNT());
    }

    @Test
    public void testDataAccessSimpleFlow() throws Exception {
        Path dummyFile = Files.createTempFile("queue", ".mv");
        FileQueueConfigVo config = new FileQueueConfigVo(dummyFile.getParent().toString(), dummyFile.getFileName().toString());
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        PartitionManager pm = new PartitionManager(storeInfo);
        DataAccess da = new DataAccess(storeInfo, pm);

        String partition = "part";
        String exec = "exec";
        da.write("tag", partition, exec, "data1");
        FileQueueData data = da.read(partition, exec);
        assertNotNull(data);
        assertEquals("data1", data.getData());
        da.readCommit(partition, exec);

        Files.deleteIfExists(dummyFile);
    }
}

