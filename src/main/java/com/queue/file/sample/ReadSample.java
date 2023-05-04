package com.queue.file.sample;

import com.queue.file.controller.Controller;
import com.queue.file.vo.FileQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ReadSample implements Runnable{
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final Controller controller;

    private final static int MAX = 10000;
    private boolean isRun = true;
    public ReadSample(Controller controller) {
        this.controller = controller;
    }

    private Map<Long, FileQueueData> cache = new LinkedHashMap<Long, FileQueueData>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, FileQueueData> eldest) {
            return size()>MAX;
        }
    };

    @Override
    public void run() {

        String name = Thread.currentThread().getName();
        long startTime = System.currentTimeMillis();
        int prcsCnt = 0;
        int sleepCnt = 0;
        while (controller.isOk() && isRun){
            try {
                List<FileQueueData> fDataList = controller.read(name);
                if(fDataList != null){
                    for(FileQueueData fData : fDataList){
                        //logger.info("<데이터 소진> - 데이터: [{}]", fData.getData());
                        if(cache.get(fData.getTransactionKey()) != null){
                            logger.info("<데이터 중복> - 키 정보: [{}], 데이터: [{}]", fData.getTransactionKey(), fData.getData());
                        }else{
                            cache.put(fData.getTransactionKey(), fData);
                        }
                    }
                    controller.readCommit(name);
                    prcsCnt += fDataList.size();
                }else{
                    logger.info("<시간 측정> - 누적 데이터 갯수: [{}], 소요시간: [{}]", prcsCnt, (System.currentTimeMillis()-startTime)-(1000*sleepCnt));
                    Thread.sleep(1000);
                    sleepCnt++;
                }
            } catch (InterruptedException e) {
                isRun = false;
            }
        }
    }
}
