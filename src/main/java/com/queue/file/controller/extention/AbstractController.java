package com.queue.file.controller.extention;

import com.google.gson.Gson;
import com.queue.file.exception.QueueReadException;
import com.queue.file.exception.QueueWriteException;
import com.queue.file.vo.extention.FileQueueCustomConfigVo;
import com.queue.file.vo.extention.FileQueueDataEx;
import org.apache.commons.lang3.ObjectUtils;
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
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractController implements ControllerEx{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final int TRANSACTION_INDEX =0;
    private final int GROUP_INDEX =1;
    private final int PARTITION_INDEX =2;
    private final int TAG_INDEX =3;
    private final int BODY_INDEX =4;
    private final int TIME_INDEX =5;


    private MVStore store = null;

    private boolean bulkCommit = false;

    private boolean manualCommitMode = false;

    private boolean encryptMode = false;

    private boolean readOnlyMode = false;

    private boolean compressMode = false;

    private boolean restoreMode = false;

    private final int LIMIT_SIZE;

    private int autoCommitDelay = 1000;
    private int autoCommitMemory = 1024*19;
    private int cacheSize = 16;
    private int bulkSize = 10;

    private MVMap<Long, String> dataMap = null;
    private MVMap<String, List<FileQueueDataEx>> readBufferMap = null;
    private final List<Long> dataKeyList = new ArrayList<Long>();

    // 처리 키 추출
    private final AtomicLong transactionIndex = new AtomicLong(1);
    // 그룹 키 추출
    private final AtomicLong groupIndex = new AtomicLong(1);

    // 파일큐 전체 경로 정보(파일 큐 포함) - QUEUE_PATH + QUEUE_NAME
    private final String QUEUE;
    // 파일큐 경로 정보(부모 디렉토리 경로)
    private final String QUEUE_PATH;
    // 파일큐 명
    private final String QUEUE_NAME;

    // 파일 암호화
    private final String ENCRYPT_KEY ="ENCRYPT_KEY";
    // 읽기 버퍼
    private final String READ_BUFFER ="READ_BUFFER_";

    private long openTime = 0;
    private long lastInTime = 0;
    private long lastOutTime = 0;
    private long waitTime = 2000;

    // 유입 건수
    private final AtomicLong INPUT_COUNT = new AtomicLong(0);

    // 처리 건수
    private final AtomicLong OUTPUT_COUNT = new AtomicLong(0);

    // 파일큐 절대 경로 정보(FULL PATH)
    public AbstractController(String queue) {
        QUEUE = queue;
        int index = 0;
        if(StringUtils.isNotBlank(queue)){
            index = queue.lastIndexOf(File.separator);
        }
        // 경로
        QUEUE_PATH = index>0?queue.substring(0,index):"";
        // 파일 명
        QUEUE_NAME = index>0?queue.substring(index+1):"";
        // 논리적 제한 사이즈
        LIMIT_SIZE = 0;
    }

    // 파일큐 위치 정보, 파일큐 이름 정보
    public AbstractController(String queuePath, String queueName) {
        QUEUE = queuePath+(queuePath.endsWith(File.separator)?"":File.separator)+queueName;
        QUEUE_PATH = queuePath;
        QUEUE_NAME = queueName;

        LIMIT_SIZE = 0;
    }

    // 파일큐 생성 설정 정보 맵
    public AbstractController(Map<String, Object> config){
        // 절대경로 (FULL PATH) = 파일큐 경로정보 + 파일큐 명
        String queue = config.get("queue")!=null?(String)config.get("queue"):"";
        // 파일큐 경로 정보
        String path = config.get("path")!=null?(String)config.get("path"):"";
        // 파일큐 명
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

        if(config.get("encryptMode")!=null) this.encryptMode = true;
        if(config.get("readOnly")!=null) this.readOnlyMode = true;
        if(config.get("compress")!=null) this.compressMode = true;

        int limit = 0;
        if(config.get("limit")!=null) {
            int var = (int)config.get("limit");
            if(var > 0) limit = var;
        }
        LIMIT_SIZE = limit;
    }

    @Override
    public boolean validate(){
        // 파일큐 경로 정보 유무 확인
        if(StringUtils.isBlank(QUEUE_PATH)){
            logger.error("<큐 활성화 : 실패> = 큐 정보:[{}], 큐 위치 정보 없음", QUEUE);
            return false;
        }

        if(StringUtils.isBlank(QUEUE_NAME)){
            logger.error("<큐 활성화 : 실패> = 큐 정보:[{}], 큐 이름 정보 없음", QUEUE);
            return false;
        }

        if(QUEUE_NAME.startsWith(READ_BUFFER)){
            logger.error("<큐 활성화 : 실패> = 큐 정보:[{}], [{}] 접두어는 사용 불가", QUEUE, READ_BUFFER);
            return false;
        }

        // 파일큐 경로 정보 유무 확인
        Path p= Paths.get(QUEUE_PATH);
        if(!Files.exists(p)) {
            try {
                Files.createFile(p);
                logger.info("<디렉토리 생성 : 성공> = 경로 정보:[{}]", QUEUE_PATH);
            } catch (IOException e) {
                logger.error("<디렉토리 생성 : 실패> = 경로 정보:[{}], 에러 발생:[{}]", QUEUE_PATH, e.getMessage());
                logger.error("<디렉토리 생성 : 실패> = 에러 상세", e);
                return false;
            }
        }

        return true;
    }

    @Override
    public synchronized boolean open() {
        // 필수 정보 검증
        if(!validate()){
            return false;
        }

        // 기존 파일 큐가 존재 하면 복구 모드로 지정
        if(Files.exists(Paths.get(QUEUE))) {
            restoreMode = true;
            logger.info("<큐 활성화 : 정보> = 큐 이름:[{}], 복구 모드", QUEUE_NAME);
        }

        try {
            // 파일큐 스토어 생성
            store = openQueue();

            // 생성 실패 시 FALSE
            if(store == null || store.isClosed()) return false;

            // 읽기 버퍼 스키마 오픈
            if(manualCommitMode){
                logger.info("<큐 활성화 : 정보> = 버퍼 큐 이름:[{}], 안정성 모드", READ_BUFFER);
                readBufferMap = store.openMap(READ_BUFFER+QUEUE_NAME);
            }

            // 데이터 스키마 오픈
            dataMap = store.openMap(QUEUE_NAME);

            if(!isOk()){
                logger.info("<큐 활성화 : 실패> = 큐:[{}]", QUEUE_NAME);
            }

            // 큐 오픈 시간 기록
            openTime = System.currentTimeMillis();

            // 파일큐 데이터 정비 및 키 정비
            if(restoreMode){
                long dataMaxKey = realignKey(dataKeyList, dataMap);
                if(dataMaxKey>0){
                    transactionIndex.set(dataMaxKey+1001);
                }
            }
        }catch(Exception e) {
            logger.error("<큐 활성화 : 실패> = 큐:[{}], 에러 발생:[{}]", QUEUE_NAME, e.toString());
            return false;
        }
        openTime = System.currentTimeMillis();
        return true;
    }

    public abstract FileQueueCustomConfigVo setConfig();

    private MVStore openQueue() {
        if(store == null || store.isClosed()) {
            HashMap<String, Object> configMap = new HashMap<>(5);
            configMap.put("fileName", QUEUE);
            if(readOnlyMode)configMap.put("readOnly", 1);
            if(compressMode)configMap.put("compress", 1);

            FileQueueCustomConfigVo configVo = setConfig();
            if(configVo == null){
                configVo = new FileQueueCustomConfigVo();
            }

            waitTime = configVo.getWaitTime();
            autoCommitDelay = configVo.getAutoCommitDelay();
            autoCommitMemory = configVo.getAutoCommitMemory();
            manualCommitMode = configVo.isManualCommitMode();
            bulkSize = configVo.getBulkSize();
            bulkCommit = configVo.isBulkCommit();
            cacheSize = configVo.getCacheSize();
            if(manualCommitMode)autoCommitDelay =0;
            // 자동 커밋 메모리 사이즈 - default 19mb
            configMap.put("autoCommitBufferSize", autoCommitMemory);

            try {
                String configInfo = DataUtils.appendMap(new StringBuilder(), configMap).toString();
                MVStore.Builder builder = MVStore.Builder.fromString(configInfo);
                if(encryptMode)builder.encryptionKey(ENCRYPT_KEY.toCharArray());
                //자동 커밋 시간 ms - default 1000ms
                store.setAutoCommitDelay(autoCommitDelay);
                //할당 메모리 사이즈 조정 - default 16mb
                store.setCacheSize(cacheSize);
                logger.info("<큐 오픈 : 성공> = 큐:[{}], 큐 설정 정보 :[{}]", QUEUE_NAME, builder);
                logger.info("<큐 오픈 : 성공> = 큐:[{}], AutoCommitDelay :[{}], AutoCommitMemory :[{}], CacheSize :[{}]", QUEUE_NAME, store.getAutoCommitDelay(), store.getAutoCommitMemory(), store.getCacheSize());
            }catch(Exception e) {
                logger.error("<큐 오픈 : 실패> = 큐:[{}], 에러 발생:[{}]", QUEUE_NAME, e.getMessage());
                logger.error("<큐 오픈 : 실패> = 에러 상세",  e);
                return null;
            }
        }
        return store;
    }

    @Override
    public long realignKey(List<Long> keyList, MVMap<Long, String> dataMap) {
        // 데이터 영역 사이즈 취득
        int mapSize = dataMap.size();
        // 키 영역 사이즈 취득
        int listSize = keyList.size();
        // 동기화 건수 확인
        int diffCnt = mapSize - listSize;
        // 키 초기 값
        long maxKey = 0;

        // 동기화 여부 확인
        if(diffCnt != 0){
            // 키 영역 클렌징
            keyList.clear();
            // 데이터 영역 기준으로 키 영역 초기화 진행
            keyList.addAll(dataMap.keySet());
            // 키 영역 정렬
            keyList.sort(Comparator.naturalOrder());
            // 동기화 건수 보정
            if(diffCnt < 0) diffCnt = diffCnt*-1;
            // 최대 값 추출 - 트랜잭션 키 초기화 값 추출
            if(!keyList.isEmpty())maxKey = keyList.get(keyList.size()-1);

            logger.info("<큐 정보 동기화 : 성공> = 큐명:[{}], 초기 KEY 갯수:[{}], 초기 DATA 화갯수:[{}], 동기화 건수:[{}]", QUEUE_NAME, listSize, mapSize, diffCnt);
        }
        return maxKey;
    }

    @Override
    public boolean isOk() {
        if((store == null || dataMap == null || store.isClosed() || dataMap.isClosed())){
            logger.error("{} 파일큐 스토어 또는 데이터 영역이 유효하지 않은 상태", QUEUE_NAME);
            return false;
        }
        if(!Files.exists(Paths.get(QUEUE))){
            logger.error("{} 파일큐를 찾을 수 없음 - 삭제 됨", QUEUE);
            return false;
        }
        if(manualCommitMode && readBufferMap.isClosed()){
            logger.error("{} 파일큐 Read Buffer 영역이 유효하지 않은 상태", QUEUE_NAME);
            return false;
        }
        return true;
    }

    @Override
    public synchronized void writeQueueData(List<String> storeDataList) throws QueueWriteException {
        if(ObjectUtils.isEmpty(storeDataList)){
            return;
        }
        boolean isBulk = storeDataList.size() >= bulkSize;
        long groupKey = getGroupKey();
        try {
            for (String storeData : storeDataList){
                long innerKey = getTransactionKey();
                dataMap.put(innerKey, innerKey+FileQueueDataEx.DELIMITER+groupKey+FileQueueDataEx.DELIMITER+storeData);
                dataKeyList.add(innerKey);
            }
            // 벌크 커밋모드 또는 수동 커밋 모드
            if((isBulk && bulkCommit)|| manualCommitMode){
                store.commit();
            }

            lastInTime = System.currentTimeMillis();
            // 데이터 영역에 데이터 입력
            addCount(storeDataList.size(), INPUT_COUNT);
            logger.debug("<큐 쓰기 : 성공> = 큐 정보:[{}], 쓰레드 명:[{}],데이타 갯수:[{}]", QUEUE_NAME, Thread.currentThread().getName(), storeDataList.size());
            if(isBulk){
                notifyAll();
            }else{
                notify();
            }
        }catch (Exception e){
            if(manualCommitMode){
                logger.error("<쓰기 : 실패> = 큐:[{}], 롤백 수행", QUEUE_NAME);
                store.rollback();
            }
            throw new QueueWriteException("<큐 쓰기 커밋 : 실패> = 큐:["+QUEUE_NAME+"], 실패 그룹키:["+groupKey+"]", e);
        }
    }

    @Override
    public synchronized List<FileQueueDataEx> read(String threadName, int requestCount) throws QueueReadException {
        List<FileQueueDataEx> queueDataList = null;
        try {
            if(manualCommitMode){
                // 읽기 영역에 기존 데이터가 있는지 확인 후 진행
                queueDataList = readBufferMap.get(threadName);
                if(ObjectUtils.isNotEmpty(queueDataList)){
                    logger.debug("<(버퍼)읽기 : 성공> = 큐:[{}], 키 정보{}], 데이터 갯수:[{}]", QUEUE_NAME, threadName, queueDataList.size());
                    return queueDataList;
                }
            }
            // 데이터 영역 데이터 건수 취득
            int mapSize = dataMap.size();
            if(mapSize < 1){
                logger.debug("<대량 읽기 : 무시> = 큐 정보:[{}], 데이터 없음", QUEUE_NAME);
                wait(waitTime);
                return null;
            }

            // 실제 전달 제한 건수 추출
            int selectCount = Math.min(requestCount, mapSize);
            boolean isBulk = selectCount >= bulkSize;
            queueDataList = new ArrayList<>(selectCount);

            // 데이터 영역 키<>데이터 동기화
            realignKey(dataKeyList, dataMap);

            // 데이터 영역에서 데이터 삭제 및 추출
            for(int i =1; i<=selectCount; i++){
                Long transKey = dataKeyList.remove(0);
                String data = dataMap.remove(transKey);
                if(transKey == null || data == null){
                    logger.warn("<읽기 : 이상> = 큐 정보:[{}], NULL 감지, 키 정보:[{}], 데이터 정보:[{}]", QUEUE_NAME, transKey, data);
                    continue;
                }
                String[] dataArray = data.split(FileQueueDataEx.DELIMITER);
                if(dataArray.length<6){
                    logger.warn("<읽기 : 이상> = 큐 정보:[{}], 데이터 이상 감지, 키 정보:[{}], 데이터 정보:[{}]", QUEUE_NAME, transKey, data);
                    continue;
                }
                long innerkey = Long.parseLong(dataArray[TRANSACTION_INDEX]);
                long groupKey = Long.parseLong(dataArray[GROUP_INDEX]);
                long time = Long.parseLong(dataArray[TIME_INDEX]);
                if(innerkey != transKey){
                    logger.warn("<읽기 : 이상> = 큐 정보:[{}], 데이터 이상 감지, 키 불일치 정보:[{}], 데이터 정보:[{}]", QUEUE_NAME, transKey, data);
                }
                FileQueueDataEx fData = new FileQueueDataEx(transKey, groupKey, dataArray[PARTITION_INDEX], dataArray[TAG_INDEX], dataArray[BODY_INDEX], time);
                queueDataList.add(fData);
            }
            if((isBulk && bulkCommit) || manualCommitMode){
                if(manualCommitMode){
                    readBufferMap.put(threadName, queueDataList);
                }
                store.commit();
            }
            lastOutTime = System.currentTimeMillis();
            logger.debug("<읽기 : 성공> = 큐:[{}], 데이터 갯수:[{}]", QUEUE_NAME, queueDataList.size());
        }catch (Exception e){
            if(manualCommitMode){
                logger.error("<읽기 : 실패> = 큐:[{}], 롤백 수행", QUEUE_NAME);
                store.rollback();
            }
            throw new QueueReadException("<읽기 : 실패> = 큐:["+QUEUE_NAME+"]", e);
        }
        return queueDataList;
    }

    private long getTransactionKey() {
        transactionIndex.compareAndSet(Long.MAX_VALUE, 1L);
        return transactionIndex.getAndIncrement();
    }
    private long getGroupKey() {
        groupIndex.compareAndSet(Long.MAX_VALUE, 1L);
        return groupIndex.getAndIncrement();
    }

    private void addCount(long count, AtomicLong atomicLong){
        atomicLong.updateAndGet(currentValue -> {
            if (currentValue >= Long.MAX_VALUE - count) {
                return count;
            }
            return currentValue + count;
        });
    }

    @Override
    public List<String> allData(){
        return new ArrayList<>(dataMap.values());
    }

    @Override
    public List<List<FileQueueDataEx>> allReadBuffer(){
        if(readBufferMap==null || !manualCommitMode){
            return null;
        }
        return new ArrayList<>(readBufferMap.values());
    }

    @Override
    public synchronized FileQueueDataEx removeOne() throws QueueReadException {
        FileQueueDataEx fData = null;
        try {
            // 데이터 영역 데이터 건수 취득
            int mapSize = dataMap.size();
            if(mapSize < 1){
                logger.info("<큐 최신 데이터 제거 : 무시> = 큐 정보:[{}], 데이터 없음", QUEUE_NAME);
                return null;
            }

            // 데이터 영역 키<>데이터 동기화
            realignKey(dataKeyList, dataMap);

            // 데이터 영역에서 1건 데이터 삭제 및 추출
            Long transKey = dataKeyList.remove(0);
            String data = dataMap.remove(transKey);

            if(transKey == null || data == null){
                logger.warn("<제거 : 이상> = 큐 정보:[{}], NULL 감지, 키 정보:[{}], 데이터 정보:[{}]", QUEUE_NAME, transKey, data);
                return null;
            }

            String[] dataArray = data.split(FileQueueDataEx.DELIMITER);
            if(dataArray.length<6){
                logger.warn("<제거 : 이상> = 큐 정보:[{}], 데이터 이상 감지, 키 정보:[{}], 데이터 정보:[{}]", QUEUE_NAME, transKey, data);
                return null;
            }
            long innerkey = Long.parseLong(dataArray[TRANSACTION_INDEX]);
            long groupKey = Long.parseLong(dataArray[GROUP_INDEX]);
            long time = Long.parseLong(dataArray[TIME_INDEX]);
            if(innerkey != transKey){
                logger.warn("<제거 : 이상> = 큐 정보:[{}], 데이터 이상 감지, 키 불일치 정보:[{}], 데이터 정보:[{}]", QUEUE_NAME, transKey, data);
            }
            fData = new FileQueueDataEx(transKey, groupKey, dataArray[PARTITION_INDEX], dataArray[TAG_INDEX], dataArray[BODY_INDEX], time);
            store.commit();
            logger.info("<큐 최신 데이터 제거 : 성공> = 큐:[{}], 데이터내용:[{}]", QUEUE_NAME, data);
        }catch (Exception e){
            throw new QueueReadException("<큐데이터 단건 제거 : 실패> = 큐:["+QUEUE_NAME+"]", e);
        }
        return fData;
    }

    @Override
    public synchronized List<FileQueueDataEx> removeReadBufferOne(String threadName) throws QueueReadException {
        List<FileQueueDataEx> fileQueueDataList = null;
        if(readBufferMap==null || !manualCommitMode){
            return null;
        }
        try {
            fileQueueDataList = readBufferMap.remove(threadName);
            if(ObjectUtils.isEmpty(fileQueueDataList)){
                logger.warn("<읽기 버퍼 제거 : 무시> = 큐:[{}], 데이터 없음", QUEUE_NAME);
                store.commit();
                return null;
            }

            logger.info("<읽기 버퍼 제거 : 성공> = 큐:[{}], 데이터 갯수:[{}]", QUEUE_NAME, fileQueueDataList.size());
            for(FileQueueDataEx queueData:fileQueueDataList){
                logger.info("삭제 데이터:{[]}", queueData.toString());
            }
            addCount(fileQueueDataList.size(), OUTPUT_COUNT);
            lastOutTime = System.currentTimeMillis();
        }catch (Exception e){
            throw new QueueReadException("<읽기 버퍼 제거: 실패> = 큐:["+QUEUE_NAME+"]", e);
        }finally {
            store.commit();
        }
        return fileQueueDataList;
    }

    @Override
    public synchronized void clear() throws QueueReadException {
        try{
            // 데이터 클렌징
            dataMap.clear();
            // 데이터 키 클렌징
            dataKeyList.clear();
            if(manualCommitMode && readBufferMap !=null){
                readBufferMap.clear();
            }
            // 저장
            store.commit();
        }catch (Exception e){
            throw new QueueReadException("<데이터 영역 클렌징 : 실패> = 큐:["+QUEUE_NAME+"]", e);
        }
    }

    @Override
    public void close() {
        if(store != null && !store.isClosed()) {
            store.commit();
            store.close();
        }
        dataKeyList.clear();
    }

    public int getQueueSize() {
        int queueSize = dataMap == null? 0 : dataMap.size();
        if(manualCommitMode && readBufferMap !=null){
            queueSize = queueSize + readBufferMap.size();
        }
        return queueSize;
    }
    public long getInputCount(){
        return INPUT_COUNT.get();
    }
    public long getOutputCount(){
        return OUTPUT_COUNT.get();
    }
    public long getLastInTime(){
        return lastInTime;
    }
    public long getLastOutTime(){ return lastOutTime;}
    public long getOpenTime() { return openTime; }

    public int getLIMIT_SIZE() { return LIMIT_SIZE; }
    public String getQUEUE() { return QUEUE; }
    public String getQUEUE_NAME() { return QUEUE_NAME; }
    public String getQUEUE_PATH() { return QUEUE_PATH; }

    public int getAutoCommitDelay() { return autoCommitDelay; }
    public int getAutoCommitMemory() { return autoCommitMemory; }
    public boolean isBulkCommit() { return bulkCommit; }
    public int getBulkSize() { return bulkSize; }
    public int getCacheSize() { return cacheSize; }

    public long getWaitTime() { return waitTime; }
    public void setWaitTime(long waitTime) { this.waitTime = waitTime; }
}
