package com.queue.file.controller;

import com.queue.file.vo.ActionType;
import com.queue.file.vo.InOutStorage;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StatsTrackerInvokeTest {

    @Test
    // 테스트 목적: outputInvokeHistoryIsRecorded 동작 확인
    public void outputInvokeHistoryIsRecorded() {
        StatsTracker tracker = new StatsTracker();
        tracker.keepRecord("consumer", ActionType.OUTPUT_INVOKE);
        InOutStorage info = tracker.getInOutInfo();
        assertTrue(info.getOutputInvokeHistoryMap().containsKey("consumer"));
    }

    @Test
    // 테스트 목적: bufferOutputInvokeHistoryIsRecorded 동작 확인
    public void bufferOutputInvokeHistoryIsRecorded() {
        StatsTracker tracker = new StatsTracker();
        tracker.keepRecord("consumer", ActionType.BUFFER_OUTPUT_INVOKE);
        InOutStorage info = tracker.getInOutInfo();
        assertTrue(info.getBufferOutputInvokeHistoryMap().containsKey("consumer"));
    }
}
