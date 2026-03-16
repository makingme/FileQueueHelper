package com.queue.file.vo;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class BufferEntryTest {

    @Test
    // 테스트 목적: touchUpdatesTimestampAndKeepsDataReference 동작 확인
    public void touchUpdatesTimestampAndKeepsDataReference() {
        List<FileQueueData> data = Arrays.asList(new FileQueueData("tag", "data"));
        long oldTs = System.currentTimeMillis() - 1000L;
        BufferEntry entry = new BufferEntry(data, oldTs);

        BufferEntry touched = entry.touch();
        assertSame(data, touched.getData());
        assertTrue(touched.getLastAccessEpochMillis() > oldTs);
    }
}
