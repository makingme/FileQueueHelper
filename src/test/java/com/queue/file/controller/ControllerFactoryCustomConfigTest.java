package com.queue.file.controller;

import com.queue.file.vo.FileQueueConfigVo;
import com.queue.file.vo.FileQueueCustomConfigVo;
import org.junit.Test;

import java.nio.file.Path;

import static org.junit.Assert.assertNotNull;

public class ControllerFactoryCustomConfigTest {

    @Test
    // 테스트 목적: createCustomControllerWithEncryptAndCompress 동작 확인
    public void createCustomControllerWithEncryptAndCompress() throws Exception {
        Path dir = TestSupport.createTempDirectory("custom");
        String queue = dir.resolve("c.mv").toString();

        FileQueueConfigVo config = new FileQueueConfigVo(queue);
        config.setEncryptMode("key");
        config.setCompressMode(false);
        config.setReadOnlyMode(false);

        FileQueueCustomConfigVo custom = new FileQueueCustomConfigVo();
        custom.setBulkCommit(true);
        custom.setBulkSize(2);
        config.setCustomConfig(custom);

        BaseController controller = ControllerFactory.createCustomController(config);
        assertNotNull(controller);
        controller.close();
        TestSupport.deleteDirectory(dir);
    }
}
