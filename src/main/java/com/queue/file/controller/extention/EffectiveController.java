package com.queue.file.controller.extention;

import com.queue.file.vo.extention.FileQueueCustomConfigVo;

import java.util.Map;

public class EffectiveController extends AbstractController{
    public EffectiveController(Map<String, Object> config) {
        super(config);
    }

    public EffectiveController(String queue) {
        super(queue);
    }

    public EffectiveController(String queuePath, String queueName) {
        super(queuePath, queueName);
    }

    @Override
    public FileQueueCustomConfigVo setConfig() {
        return new FileQueueCustomConfigVo();
    }
}
