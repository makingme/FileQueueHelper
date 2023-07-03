package com.queue.file.sample;

import com.queue.file.controller.Controller;
import com.queue.file.exception.QueueWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class WriterSample implements Runnable{
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final Controller controller;

    private boolean isRun = true;
    public WriterSample(Controller controller) {
        this.controller = controller;
    }

    @Override
    public void run() {
        final Map<String, Object> dataMap = new HashMap<>(2);
        int loopCnt = 10000;
        String name = Thread.currentThread().getName();
        StringBuilder sb = new StringBuilder();
        for(int j=1; j<=1000; j++){
            sb.append("1234567890");
        }
        String data = sb.toString();
        long startTime = System.currentTimeMillis();
        for(int i =1; i<=loopCnt; i++){
            try {
                dataMap.put("DATA", data+i);
                controller.write(dataMap);
                //Thread.sleep(1000);
                //logger.info("<데이터 입력> - 데이터 정보: [{}]", i);
            }catch (QueueWriteException e) {
                isRun = false;
                throw new RuntimeException(e);
            }
        }
        logger.info("<데이터 입력 - 완료> - 데이터 갯수: [{}], 소요시간: [{}]", loopCnt, (System.currentTimeMillis()-startTime));
    }
}
