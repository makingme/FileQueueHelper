package com.queue.file.sample;

import com.queue.file.controller.BaseController;
import com.queue.file.controller.ControllerFactory;
import com.queue.file.exception.QueueReadException;
import com.queue.file.exception.QueueWriteException;
import com.queue.file.vo.FileQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Simple load test using BaseController with asynchronous producers and consumers.
 */
public class LoadTest {
    private static final Logger logger = LoggerFactory.getLogger(LoadTest.class);

    private static final int PRODUCER_COUNT = 5;
    private static final int CONSUMER_COUNT = 3;

    private static final java.util.concurrent.atomic.AtomicLong producedCount = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong consumedCount = new java.util.concurrent.atomic.AtomicLong();

    public static void main(String[] args) throws Exception {
        Path file = Files.createTempFile("filequeue-load", ".mv");
        BaseController controller = ControllerFactory.create(file.toString());

        ThreadFactory producerFactory = new ThreadFactory() {
            private int n = 0;

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "producer-" + n++);
            }
        };

        ExecutorService producerPool = Executors.newFixedThreadPool(PRODUCER_COUNT, producerFactory);

        for (int i = 0; i < PRODUCER_COUNT; i++) {
            final int id = i;
            CompletableFuture.runAsync(() -> produce(controller, id), producerPool);
        }

        List<ConsumerThread> consumers = new ArrayList<>();
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            ConsumerThread t = new ConsumerThread(controller, "consumer-" + i);
            consumers.add(t);
            t.start();
        }

        long prevProduced = 0;
        long prevConsumed = 0;
        long lastCheck = System.currentTimeMillis();
        while (true) {
            Thread.sleep(1000);
            long now = System.currentTimeMillis();
            long interval = now - lastCheck;
            long p = producedCount.get();
            long c = consumedCount.get();
            long pDelta = p - prevProduced;
            long cDelta = c - prevConsumed;
            double inTps = interval > 0 ? (pDelta * 1000.0) / interval : 0.0;
            double outTps = interval > 0 ? (cDelta * 1000.0) / interval : 0.0;
            logger.info("produced: {} (+{}), consumed: {} (+{}), input TPS: {}, output TPS: {}",
                    p, pDelta, c, cDelta, String.format("%.2f", inTps), String.format("%.2f", outTps));
            prevProduced = p;
            prevConsumed = c;
            lastCheck = now;
        }
    }

    private static void produce(BaseController controller, int id) {
        int count = 0;
        while (true) {
            try {
                controller.write("p" + id + "-" + count++);
                producedCount.incrementAndGet();
                // 짧은 휴식으로 CPU 사용 줄이기
                if (count % 100 == 0) {
                    Thread.sleep(1);
                }
            } catch (QueueWriteException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private static class ConsumerThread extends Thread {
        private final BaseController controller;

        ConsumerThread(BaseController controller, String name) {
            super(name);
            this.controller = controller;
        }

        @Override
        public void run() {
            int emptyCount = 0;
            String executor = getName();
            while (true) {
                try {
                    List<FileQueueData> list = controller.read(executor, 20);
                    if (list != null && !list.isEmpty()) {
                        controller.readCommit(executor);
                        consumedCount.addAndGet(list.size());
                        emptyCount = 0;
                    } else {
                        emptyCount = (emptyCount + 1) % Integer.MAX_VALUE;
                        Thread.sleep(100);
                    }
                } catch (QueueReadException | InterruptedException e) {
                    break;
                }
            }
        }
    }
}
