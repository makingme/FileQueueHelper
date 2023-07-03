package com.queue.file.sample;

import com.queue.file.controller.Controller;
import com.queue.file.exception.QueueReadException;
import com.queue.file.vo.FileQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ReadThrowSample implements Runnable{
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final Controller controller;

    private boolean isRun = true;

    public ReadThrowSample(Controller controller) {
        this.controller = controller;
    }

    @Override
    public void run() {
        int loopCnt = 1;
        String name = Thread.currentThread().getName();
        while (controller.isOk() && isRun){
            try {
                List<FileQueueData> fDataList = controller.read(name);
                Thread.sleep(500);
                if(fDataList != null){
                    for(FileQueueData fData : fDataList){
                        logger.info("<데이터 확인> - 데이터: [{}]", fData);
                    }
                    controller.readCommit(name);
                }
            } catch (InterruptedException e) {
                isRun = false;
            } catch (QueueReadException e) {
                isRun = false;
                throw new RuntimeException(e);
            }
        }
    }
}
