package com.queue.file;

import com.queue.file.controller.ManualController;
import com.queue.file.sample.BulkReadSample;
import com.queue.file.sample.BulkWriterSample;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestMain {
    public static void main(String[] args) throws IOException {
        String fileName = "/Users/mz01-kibumkim/Desktop/DATA/queue/TestMain.fq";

        Path path = Paths.get(fileName);
        if(Files.exists(path)){
            Files.delete(path);
        }

        // 큐 관리 모드 설정
        Map<String, Object> configMap = new HashMap<String, Object>(5);
        configMap.put("queue", fileName);
        // 수동 커밋
        configMap.put("manualCommitMode", 1);
        // 암호화 키 지정 시 암호화 모드 적용
        configMap.put("encryptMode", 1);
        // 읽기 전용 모드로 열기
        //configMap.put("readOnly", 1);
        // 압축 모드 지정 1:압축 2:고압축
        configMap.put("compress", 1);

        ManualController controller = new ManualController(configMap);

        controller.open();
//        Thread w1 = new Thread(new WriterSample(controller), "W1");
//        Thread w2 = new Thread(new WriterSample(controller), "W2");
//        Thread w3 = new Thread(new WriterSample(controller), "W3");

        Thread w1 = new Thread(new BulkWriterSample(controller), "W1");
        Thread w2 = new Thread(new BulkWriterSample(controller), "W2");
        Thread w3 = new Thread(new BulkWriterSample(controller), "W3");

//        Thread r1 = new Thread(new ReadSample(controller), "R1");
//        Thread r2 = new Thread(new ReadSample(controller), "R2");
//        Thread r3 = new Thread(new ReadSample(controller), "R3");

        Thread r1 = new Thread(new BulkReadSample(controller), "R1");
        Thread r2 = new Thread(new BulkReadSample(controller), "R2");
        Thread r3 = new Thread(new BulkReadSample(controller), "R3");

        w1.start();
        w2.start();
//        w3.start();
        //Thread r1 = new Thread(new ReadThrowSample(controller), "R1");
        r1.start();
       r2.start();
//        r3.start();
    }
}
