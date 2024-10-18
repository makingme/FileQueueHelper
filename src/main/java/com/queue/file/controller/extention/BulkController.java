package com.queue.file.controller.extention;

import com.queue.file.vo.extention.FileQueueCustomConfigVo;

import java.util.Map;

public class BulkController extends AbstractController{
    private final int BULK_SIZE;
    public BulkController(Map<String, Object> config, int BULK_SIZE) {
        super(config);
        this.BULK_SIZE = BULK_SIZE;
    }

    public BulkController(String queue, int BULK_SIZE) {
        super(queue);
        this.BULK_SIZE = BULK_SIZE;
    }

    public BulkController(String queuePath, String queueName, int BULK_SIZE) {
        super(queuePath, queueName);
        this.BULK_SIZE = BULK_SIZE;
    }

    @Override
    public FileQueueCustomConfigVo setConfig() {
        FileQueueCustomConfigVo vo = new FileQueueCustomConfigVo();
        vo.setBulkSize(BULK_SIZE);
        vo.setBulkCommit(true);
        return vo;
    }
}
