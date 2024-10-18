package com.queue.file.controller.extention;

import com.queue.file.vo.extention.FileQueueCustomConfigVo;

import java.util.Map;

public class CustomController extends AbstractController{
    private final FileQueueCustomConfigVo CUSTOM_CONFIG;
    public CustomController(Map<String, Object> config, FileQueueCustomConfigVo fileQueueCustomConfigVo) {
        super(config);
        CUSTOM_CONFIG = fileQueueCustomConfigVo;
    }

    public CustomController(String queue, FileQueueCustomConfigVo fileQueueCustomConfigVo) {
        super(queue);
        CUSTOM_CONFIG = fileQueueCustomConfigVo;
    }

    public CustomController(String queuePath, String queueName, FileQueueCustomConfigVo fileQueueCustomConfigVo) {
        super(queuePath, queueName);
        CUSTOM_CONFIG = fileQueueCustomConfigVo;
    }

    @Override
    public FileQueueCustomConfigVo setConfig() {
        if (CUSTOM_CONFIG == null) {
            return new FileQueueCustomConfigVo();
        }
        return CUSTOM_CONFIG;
    }
}
