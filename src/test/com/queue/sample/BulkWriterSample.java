package com.queue.file.sample;

import com.queue.file.controller.BaseController;
import com.queue.file.exception.QueueWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkWriterSample implements Runnable{
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final BaseController controller;

    private boolean isRun = true;

    public BulkWriterSample(BaseController controller) {
        this.controller = controller;
    }

    @Override
    public void run() {
        final Map<String, Object> dataMap = new HashMap<>(2);
        int loopCnt = 50;
        String name = Thread.currentThread().getName();
        StringBuilder sb = new StringBuilder();
        for(int j=1; j<=1000; j++){
            sb.append("1234567890");
        }
        String data = sb.toString();

        long startTime = System.currentTimeMillis();
        for(int i =1; i<=loopCnt; i++){
            try {
                List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>(200);
                for(int x=1; x<=200; x++){
                    dataMap.put("DATA", i+data+x);
                    dataList.add(dataMap);
                }
                controller.write("");
                //Thread.sleep(1000);
                //logger.info("<데이터 입력> - 데이터 정보: [{}]", i);
            } catch (QueueWriteException e) {
                isRun = false;
                throw new RuntimeException(e);
            }
        }
        logger.info("<데이터 입력 - 완료> - 데이터 갯수: [{}], 소요시간: [{}]", loopCnt, (System.currentTimeMillis()-startTime));
    }
}
