package com.queue.file.manager;

import com.queue.file.controller.Controller;
import com.queue.file.controller.ManualController;
import com.queue.file.vo.FileQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ControlManager {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final Map<String, Controller> CONTROL_MAP = new HashMap<String, Controller>();

    public synchronized Controller openQueue(FileQueueBuilder builder) {
        if(builder == null || builder.getConfig() == null) {
            logger.error("<큐 활성화 : 실패> = 사유: 큐 설정 정보 NULL");
            return null;
        }
        return openQueue(builder.getConfig());
    }

    public synchronized Controller openQueue(Map<String, Object> config) {
        Controller controller = new ManualController(config);
        if(controller.validate() == false) return null;

        String queueName = controller.getQueueName();
        if(getController(queueName) != null){
            logger.error("<큐 활성화 : 실패> = 큐 이름: [{}], 사유: 큐 이름 중복", queueName);
            return null;
        }

        if(controller.open()==false){
            return null;
        }

        CONTROL_MAP.put(queueName, controller);
        return controller;
    }

    public Controller getController(String queueName) {
        Controller helper = CONTROL_MAP.get(queueName);
        if(helper != null && helper.isOk()) return helper;
        return null;
    }

    public void closeAll(){
        for(String queueName : CONTROL_MAP.keySet()){
            close(queueName);
        }
    }

    public void close(String queueName){
        Controller controller = CONTROL_MAP.get(queueName);
        if(controller !=null)controller.close();
    }

}
