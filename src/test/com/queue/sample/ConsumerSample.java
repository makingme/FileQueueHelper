package com.queue.file.sample;

import com.queue.file.controller.BaseController;
import com.queue.file.exception.QueueReadException;
import com.queue.file.vo.FileQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Sample consumer that reads messages using BaseController.
 */
public class ConsumerSample implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerSample.class);

    private final BaseController controller;
    private volatile boolean run = true;

    public ConsumerSample(BaseController controller) {
        this.controller = controller;
    }

    @Override
    public void run() {
        String executor = Thread.currentThread().getName();
        while (run) {
            try {
                List<FileQueueData> list = controller.read(executor, 5);
                if (list != null && !list.isEmpty()) {
                    for (FileQueueData data : list) {
                        logger.info("consumed: {}", data.getData());
                    }
                    controller.readCommit(executor);
                    break; // exit after one batch for sample
                }
                Thread.sleep(500);
            } catch (InterruptedException e) {
                run = false;
            } catch (QueueReadException e) {
                run = false;
                throw new RuntimeException(e);
            }
        }
    }
}
