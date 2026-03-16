package com.queue.file.vo;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class FileQueueConfigVoTest {

    @Test
    // 테스트 목적: parsesQueuePathAndNameFromFullPath 동작 확인
    public void parsesQueuePathAndNameFromFullPath() {
        String queue = "C:" + File.separator + "tmp" + File.separator + "q.mv";
        FileQueueConfigVo config = new FileQueueConfigVo(queue);
        assertEquals("C:" + File.separator + "tmp", config.getQueuePath());
        assertEquals("q.mv", config.getQueueName());
        assertEquals(queue, config.getQueue());
    }

    @Test
    // 테스트 목적: noSeparatorResultsInEmptyPathAndName 동작 확인
    public void noSeparatorResultsInEmptyPathAndName() {
        FileQueueConfigVo config = new FileQueueConfigVo("queue.mv");
        assertEquals("", config.getQueuePath());
        assertEquals("", config.getQueueName());
        assertEquals("queue.mv", config.getQueue());
    }

    @Test
    // 테스트 목적: concatenatesQueuePathAndName 동작 확인
    public void concatenatesQueuePathAndName() {
        FileQueueConfigVo config = new FileQueueConfigVo("C:" + File.separator + "tmp", "q.mv");
        assertEquals("C:" + File.separator + "tmp", config.getQueuePath());
        assertEquals("q.mv", config.getQueueName());
        assertEquals("C:" + File.separator + "tmp" + File.separator + "q.mv", config.getQueue());
    }
}
