package com.queue.file.vo;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class QueueSummaryVoTest {

    @Test
    // 테스트 목적: toStringIncludesCounts 동작 확인
    public void toStringIncludesCounts() {
        QueueSummaryVo summary = new QueueSummaryVo(1, 2, 3);
        String text = summary.toString();
        assertTrue(text.contains("dataCount=1"));
        assertTrue(text.contains("bufferCount=2"));
        assertTrue(text.contains("cacheCount=3"));
    }
}
