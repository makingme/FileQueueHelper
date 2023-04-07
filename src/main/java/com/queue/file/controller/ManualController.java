package com.queue.file.controller;

import com.queue.file.vo.FileQueueData;
import org.apache.commons.lang3.StringUtils;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class ManualController implements Controller{
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private MVStore store = null;

    private boolean manualCommitMode = false;

    private boolean encryptMode = false;

    private boolean readOnlyMode = false;

    private boolean compressMode = false;

    private boolean restoreMode = false;


    private int MAX_WRITER = 1;
    private int MAX_READER =1;

    Semaphore readSema = new Semaphore(MAX_READER, true);

    Semaphore dataSema = new Semaphore(1, true);


    private Map<Long, String> writeBufferMap = null;

    private Map<Long, List<FileQueueData>> writeBulkBufferMap = null;

    private MVMap<Long, String> dataMap = null;
    private MVMap<Long, String> readBufferMap = null;

    private final List<Long> dataKeyList = new ArrayList<Long>();

    private final AtomicLong orderIndex = new AtomicLong(0);
    private final AtomicLong transIndex = new AtomicLong(1);

    private final String QUEUE;
    private final String QUEUE_PATH;
    private final String QUEUE_NAME;
    private final String ENCRYPT_KEY ="ENCRYPT_KEY";

    private final String WRITE_BUFFER ="WRITEBUFFER_";
    private final String READ_BUFFER ="READBUFFER_";

    private long openTime = 0;
    private long lastInTime = 0;
    private long lastOutTime = 0;

    private long inputCnt =0;
    private long outputCnt =0;

    // 파일 큐 위치/이름 정보
    public ManualController(String queue) {
        QUEUE = queue;
        int index = 0;
        if(StringUtils.isNotBlank(queue)){
            index = queue.lastIndexOf(File.separator);
        }
        QUEUE_PATH = index>0?queue.substring(0,index):"";
        QUEUE_NAME = index>0?queue.substring(index+1):"";
    }

    // 파일큐 위치 정보, 파일큐 이름 정보
    public ManualController(String queuePath, String queueName) {
        QUEUE = queuePath+(queuePath.endsWith(File.separator)?"":File.separator)+queueName;
        QUEUE_PATH = queuePath;
        QUEUE_NAME = queueName;
    }

    // 파일큐 생성 설정 정보 맵
    public ManualController(Map<String, Object> config){

        String queue = config.get("queue")!=null?(String)config.get("queue"):"";
        String path = config.get("path")!=null?(String)config.get("path"):"";
        String name = config.get("name")!=null?(String)config.get("name"):"";

        if(StringUtils.isBlank(queue)){
            queue = path+(path.endsWith(File.separator)?"":File.separator)+name;
        }else{
            int index = queue.lastIndexOf(File.separator);;
            path = index>0?queue.substring(0,index):"";
            name = index>0?queue.substring(index+1):"";
        }

        QUEUE = queue;
        QUEUE_PATH = path;
        QUEUE_NAME = name;

        if(config.get("manualCommitMode")!=null)this.manualCommitMode = true;
        if(config.get("encryptMode")!=null) this.encryptMode = true;
        if(config.get("readOnly")!=null) this.readOnlyMode = true;
        if(config.get("compress")!=null) this.compressMode = true;
    }

    @Override
    public synchronized boolean open() {
        // 파일큐 경로 정보 유무 확인
        if(StringUtils.isBlank(QUEUE_PATH)){
            logger.error("입력된 퍄일 큐 위치 정보가 없습니다.");
            return false;
        }

        if(StringUtils.isBlank(QUEUE_NAME)){
            logger.error("입력된 퍄일 큐 이름 정보가 없습니다.");
            return false;
        }

        Path p= Paths.get(QUEUE_PATH);
        if(Files.exists(p) == false) {
            try {
                Files.createFile(p);
                logger.info("퍄일 큐 저장 디렉토리 생성 - {}", QUEUE_PATH);
            } catch (IOException e) {
                logger.error("퍄일 큐 저장 디렉토리 생성({}) 중 에러 발생:{}",QUEUE_PATH, e);
                return false;
            }
        }

        if(QUEUE_NAME.startsWith(WRITE_BUFFER)||QUEUE_NAME.startsWith(READ_BUFFER)){
            logger.error("{}, {} 접두어는 사용 불가 - 큐 생성 실패", WRITE_BUFFER, READ_BUFFER);
            return false;
        }

        // 기존 파일 큐가 존재 하면 복구 모드로 지정
        if(Files.exists(Paths.get(QUEUE))) {
            restoreMode = true;
            logger.info("기존 퍄일 큐 확인({}), 복구 모드 활성화", QUEUE);
        }

        try {
            // 파일큐 스토어 생성
            store = openQueue();

            // 생성 실패 시 FALSE
            if(store == null || store.isClosed()) return false;

            // 쓰기 버퍼 스키마 오픈
            writeBufferMap = new ConcurrentHashMap<Long, String>();
            writeBulkBufferMap = new ConcurrentHashMap<Long, List<FileQueueData>>();

            // 읽기 버퍼 스키마 오픈
            readBufferMap = store.openMap(READ_BUFFER+QUEUE_NAME);
            // 데이터 스키마 오픈
            dataMap = store.openMap(QUEUE_NAME);

            if(isOk() == false){
                logger.info("{} 큐 활성화 실패");
            }

            // 큐 오픈 시간 기록
            openTime = System.currentTimeMillis();

            // 파일큐 데이터 정비 및 키 정비
            if(restoreMode){
                realignData();
                long dataMaxKey = realignKey(dataKeyList, dataMap);
                orderIndex.set(dataMaxKey+1000);
            }
        }catch(Exception e) {
            logger.error(QUEUE_PATH+" 큐 활성화 중 에러 발생:"+e);
            return false;
        }

        return true;
    }

    private MVStore openQueue() {
        if(store == null || store.isClosed()) {
            HashMap<String, Object> configMap = new HashMap<String, Object>(5);
            configMap.put("fileName", QUEUE);
            if(manualCommitMode)configMap.put("autoCommitDelay", 0);
            if(readOnlyMode)configMap.put("readOnly", 1);
            if(compressMode)configMap.put("compress", 1);

            try {
                String configInfo = DataUtils.appendMap(new StringBuilder(), configMap).toString();
                MVStore.Builder builder = new MVStore.Builder().fromString(configInfo);
                if(encryptMode)builder.encryptionKey(ENCRYPT_KEY.toCharArray());
                logger.info("파일 큐 설정 정보 : {}", builder.toString());
                store = builder.open();
            }catch(Exception e) {
                logger.error("{} 파일 큐 오픈 중 에러 발생:{}", QUEUE, e);
                return null;
            }
        }
        return store;
    }


    @Override
    public void realignData() throws InterruptedException{
        try{
            // 데이터/읽기 영역 통제
            dataSema.acquire();
            readSema.acquire(MAX_READER);

            // 쓰기 버퍼 영역 데이터는 삭제 함
            if(writeBufferMap.size()>0){
                writeBufferMap.clear();
            }

            // 읽기 버퍼 영역 데이터는 데이터 영역으로 편입
            if(readBufferMap.size()>0){
                for(long key : readBufferMap.keySet()){
                    String data = readBufferMap.remove(key);
                    dataMap.put(key, data);
                }
            }
            // 데이터 이관 정리
            store.commit();

        }catch (Exception e){
            throw new InterruptedException();
        }finally {
            // 데이터/읽기 영역 통제 해제
            dataSema.acquire();
            readSema.release(MAX_READER);
        }
    }

    @Override
    public long realignKey(List<Long> keyList, MVMap<Long, String> dataMap) {
        // KEY SYNC
        int mapSize = dataMap.size();
        int listSize = keyList.size();
        int diffCnt = mapSize - listSize;
        long maxKey = 1;

        if(diffCnt > 0){
            keyList.clear();
            for(Long k: dataMap.keySet()) {
                keyList.add(k);
            }
            Collections.sort(keyList, (a, b)->a.compareTo(b));
            if(diffCnt < 0) diffCnt = diffCnt*-1;
            logger.warn("인덱스키 동기화 {}건 진행 - KEY DATA:{}, QUEUE DATA:{}", diffCnt, listSize, mapSize);
            if(keyList.size()>0)maxKey = keyList.get(keyList.size()-1);
        }
        return maxKey;
    }

    @Override
    public long write(String data) throws InterruptedException{
        if(FileQueueData.isValid(data) == false) {
            logger.warn("쓰기 요청 데이터 구성 오류 - {}", data);
            return -1;
        }

        long tranKey = getNextTransKey();
        writeBufferMap.put(tranKey, data.toString());

        return tranKey;
    }

    @Override
    public long write(FileQueueData data) throws InterruptedException {

        long transKey = getNextTransKey();
        writeBufferMap.put(transKey, data.toString());

        return transKey;
    }


    public long write(List<FileQueueData> dataList) throws InterruptedException {

        long transKey = getNextTransKey();
        writeBulkBufferMap.put(transKey, dataList);

        return transKey;
    }

    public void writeCommit(long transKey){
        try {
            // 데이터 영역 통제
            dataSema.acquire();
            String data = writeBufferMap.get(transKey);

            dataMap.put(transKey, data);
            dataKeyList.add(transKey);

            writeBufferMap.remove(transKey);
            store.commit();
            // 데이터 영역 통제 해제
            dataSema.release();
            logger.debug("W-COMMIT 수행 - {} 큐, DATA : [{}]", QUEUE_NAME, data);
        }catch (Exception e){
            logger.error(e.toString());
        }
    }

    public void writeRollback(long indexKey){
        try {
            // 데이터 영역 통제
            writeSema.acquire();

            String data = writeBufferMap.remove(indexKey);

            store.commit();
            // 데이터 영역 통제 해제
            writeSema.release();
            logger.debug("W-ROLLBACK 수행 - {} 큐, DATA : [{}]", QUEUE_NAME, data);
        }catch (Exception e){
            logger.error(e.toString());
        }
    }

    private synchronized long getNextDataKey() {
        long index = orderIndex.getAndAdd(2L);

        if(index >= Long.MAX_VALUE) {
            index = 2;
            orderIndex.set(index);
        }
        return index;
    }

    private synchronized long getNextTransKey() {
        long index =transIndex.getAndAdd(2L);

        if(index >= Long.MAX_VALUE) {
            index = 3;
            transIndex.set(index);
        }
        return index;
    }

    @Override
    public FileQueueData read() throws InterruptedException {
        FileQueueData queueData = null;
        // 데이터 영역 통제
        dataSema.acquire();
        if(dataMap.size()<=0)return null;
        // 데이터 영역 키<>데이터 동기화
        realignKey(dataKeyList, dataMap);

        //키와 데이터 추출
        Long key = dataKeyList.get(0);
        String data = dataMap.get(key);

        // 큐 데이터 생성
        queueData = FileQueueData.fromString(data);

        // 큐 데이터가 정상일 경우만 읽기 버퍼 영역에 저장
        if(queueData != null){
            readBufferMap.put(key, data);
        }

        // 데이터 영역에서 제거
        dataKeyList.remove(key);
        dataMap.remove(key);

        store.commit();

        // 데이터 영역 통제 해제
        dataSema.release();
        return queueData;
    }

    public void readCommit(long indexKey){
        try {
            // 읽기 버퍼 영역 통제
            readSema.acquire();

            String data = readBufferMap.remove(indexKey);

            store.commit();
            // 읽기 버퍼 영역 통제 해제
            readSema.release();
            logger.debug("R-COMMIT 수행 - {} 큐, DATA : [{}], write commit 수행", QUEUE_NAME, data);
        }catch (Exception e){
            logger.error(e.toString());
        }
    }

    public void readRollBack(long indexKey){
        try {
            // 데이터 영역 통제
            dataSema.acquire();
            String data = readBufferMap.get(indexKey);

            dataMap.put(indexKey, data);
            dataKeyList.add(indexKey);
            Collections.sort(dataKeyList, (a, b)->a.compareTo(b));

            readBufferMap.remove(indexKey);
            store.commit();
            // 데이터 영역 통제 해제
            dataSema.release();
            logger.debug("R-ROLLBACK 수행 - {} 큐, DATA : [{}]", QUEUE_NAME, data);
        }catch (Exception e){
            logger.error(e.toString());
        }
    }

    @Override
    public boolean isOk() {
        if((store == null || dataMap == null || store.isClosed() || dataMap.isClosed()))return false;
        return (readBufferMap == null || writeBufferMap == null || readBufferMap.isClosed() || writeBufferMap.isClosed())?false:true;
    }

    @Override
    public String getQueue() {
        return QUEUE;
    }

    @Override
    public String getQueueName() {
        return QUEUE_NAME;
    }

    @Override
    public String getQueuePath() {
        return QUEUE_PATH;
    }
}
