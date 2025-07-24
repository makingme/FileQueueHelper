package com.queue.file.sample;

import com.queue.file.controller.BaseController;
import com.queue.file.exception.QueueWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample producer that writes simple string messages using BaseController.
 */
public class ProducerSample implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ProducerSample.class);

    private final BaseController controller;
    private volatile boolean run = true;

    public ProducerSample(BaseController controller) {
        this.controller = controller;
    }

    @Override
    public void run() {
        int count = 0;
        while (run && count < 100) {
            try {
                String data = "sample-" + count;
                controller.write(data);
                count++;
            } catch (QueueWriteException e) {
                run = false;
                throw new RuntimeException(e);
            }
        }
        logger.info("producer finished: {} messages", count);
    }
}
