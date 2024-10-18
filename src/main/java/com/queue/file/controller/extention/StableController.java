package com.queue.file.controller.extention;

import com.queue.file.vo.extention.FileQueueCustomConfigVo;

import java.util.Map;

public class StableController extends AbstractController{
    public StableController(Map<String, Object> config) {
        super(config);
    }

    public StableController(String queue) {
        super(queue);
    }

    public StableController(String queuePath, String queueName) {
        super(queuePath, queueName);
    }

    @Override
    public FileQueueCustomConfigVo setConfig() {
        FileQueueCustomConfigVo vo = new FileQueueCustomConfigVo();
        vo.setManualCommitMode(true);
        return vo;
    }
}
