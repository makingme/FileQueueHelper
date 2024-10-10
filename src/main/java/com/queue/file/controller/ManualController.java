package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import com.queue.file.exception.QueueReadException;
import com.queue.file.exception.QueueWriteException;
import com.queue.file.vo.FileQueueData;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
//TODO:: 메모리 캐쉬 기능 제공 및 캐쉬 파일큐 저장 기능 추가, OOME 일 경우 트랜잭션 이상 현상 체크
public class ManualController implements Controller{
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private MVStore store = null;

    private boolean manualCommitMode = true;

    private boolean encryptMode = false;

    private boolean readOnlyMode = false;

    private boolean compressMode = false;

    private boolean restoreMode = false;

    private final int MAX_READER;

    private final int LIMIT_SIZE;

    private final Semaphore readSema ;

    private final Semaphore dataSema = new Semaphore(1, true);

    private MVMap<Long, String> dataMap = null;
    private MVMap<String, List<FileQueueData>> readBufferMap = null;
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

    // 유입 건수
    private final AtomicLong INPUT_COUNT = new AtomicLong(0);

    // 처리 건수
    private final AtomicLong OUTPUT_COUNT = new AtomicLong(0);

    // 파일 큐 위치/이름 정보
    public ManualController(String queue) {
        QUEUE = queue;
        int index = 0;
        if(StringUtils.isNotBlank(queue)){
            index = queue.lastIndexOf(File.separator);
        }
        QUEUE_PATH = index>0?queue.substring(0,index):"";
        QUEUE_NAME = index>0?queue.substring(index+1):"";

        MAX_READER = 1;
        LIMIT_SIZE = 0;

        readSema = new Semaphore(MAX_READER, true);
    }

    // 파일큐 위치 정보, 파일큐 이름 정보
    public ManualController(String queuePath, String queueName) {
        QUEUE = queuePath+(queuePath.endsWith(File.separator)?"":File.separator)+queueName;
        QUEUE_PATH = queuePath;
        QUEUE_NAME = queueName;

        MAX_READER = 1;
        LIMIT_SIZE = 0;

        readSema = new Semaphore(MAX_READER, true);
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
        // TODO: 추후 manualCommitMode 값에 따라 구현 체가 분기 되도록 지원
        if(config.get("manualCommitMode")!=null)this.manualCommitMode = true;
        if(config.get("encryptMode")!=null) this.encryptMode = true;
        if(config.get("readOnly")!=null) this.readOnlyMode = true;
        if(config.get("compress")!=null) this.compressMode = true;
        int limit = 0, multiRead = 1;
        if(config.get("limit")!=null) {
            int var = (int)config.get("limit");
            if(var > 0) limit = var;
        }
        if(config.get("multiRead")!=null) {
            int var = (int)config.get("multiRead");
            if(var > 1) multiRead = var;
        }

        LIMIT_SIZE = limit;
        MAX_READER = multiRead;

        readSema = new Semaphore(MAX_READER, true);
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
            logger.error("<큐 활성화 : 실패> = [{}] 접두어는 사용 불가", READ_BUFFER);
            return false;
        }
        return true;
    }

    @Override
    public synchronized boolean open() {

        // 멤버 변구 검증
        if(validate() == false){
            return false;
        }

        // 파일큐 경로 정보 유무 확인
        Path p= Paths.get(QUEUE_PATH);
        if(Files.exists(p) == false) {
            try {
                Files.createFile(p);
                logger.info("<디렉토리 생성 : 성공> = 경로 정보:[{}]", QUEUE_PATH);
            } catch (IOException e) {
                logger.error("<디렉토리 생성 : 실패> = 경로 정보:[{}], 에러 발생:[{}]", QUEUE_PATH, e);
                return false;
            }
        }

        // 기존 파일 큐가 존재 하면 복구 모드로 지정
        if(Files.exists(Paths.get(QUEUE))) {
            restoreMode = true;
            logger.debug("<큐 활성화 : 정보> = 큐 이름:[{}], 복구 모드 활성화", QUEUE_NAME);
        }

        try {
            // 파일큐 스토어 생성
            store = openQueue();

            // 생성 실패 시 FALSE
            if(store == null || store.isClosed()) return false;

            // 읽기 버퍼 스키마 오픈
            readBufferMap = store.openMap(READ_BUFFER+QUEUE_NAME);

            // 데이터 스키마 오픈
            dataMap = store.openMap(QUEUE_NAME);

            if(isOk() == false){
                logger.info("<큐 활성화 : 실패> = 큐:[{}]", QUEUE_NAME);
            }

            // 큐 오픈 시간 기록
            openTime = System.currentTimeMillis();

            // 파일큐 데이터 정비 및 키 정비
            if(restoreMode){
                realignData();
                long dataMaxKey = realignKey(dataKeyList, dataMap);
                if(dataMaxKey>0){
                    transactionIndex.set(dataMaxKey+1001);
                }
            }
        }catch(Exception e) {
            logger.error("<큐 활성화 : 실패> = 큐:[{}], 에러 발생:[{}]", QUEUE_NAME, e.toString());
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
                //OffHeapStore offHeap = new OffHeapStore();
                MVStore.Builder builder = new MVStore.Builder().fromString(configInfo);
                if(encryptMode)builder.encryptionKey(ENCRYPT_KEY.toCharArray());
                store = builder.open();
                //store.setCacheSize(5);  할당 메모리 사이즈 조정 - default 16mb
                logger.info("<큐 오픈 : 성공> = 큐:[{}], 큐 설정 정보 :[{}]", QUEUE_NAME, builder.toString());
            }catch(Exception e) {
                logger.error("<큐 오픈 : 실패> = 큐:[{}], 에러 발생:[{}]", QUEUE_NAME, e.getMessage());
                logger.error("<큐 오픈 : 실패> = 에러 상세",  e);
                return null;
            }
        }
        return store;
    }


    @Override
    public void realignData() throws InitializeException {
        try{
            // 데이터/읽기 영역 통제
            dataSema.acquire();
            readSema.acquire(MAX_READER);

            // 읽기 버퍼 영역 데이터는 데이터 영역으로 편입
            int targetDataSize = readBufferMap.size();
            if(targetDataSize>0){
                for(String threadName : readBufferMap.keySet()){
                    List<FileQueueData> fileQueueDataList = readBufferMap.remove(threadName);
                    for (FileQueueData fData : fileQueueDataList){
                        dataKeyList.add(fData.getTransactionKey());
                        dataMap.put(fData.getTransactionKey(), fData.toString());
                    }
                }
                logger.info("<큐 데이터 이관 : 성공> = 큐:[{}], 이관 읽기 버퍼 데이터 갯수:[{}]", QUEUE_NAME, targetDataSize);
                // 데이터 이관 정리
                store.commit();
            }

        }catch (Exception e){
            store.rollback();
            throw new InitializeException("<큐 데이터 정렬 : 실패> = 큐 롤백 수행, 큐:["+QUEUE_NAME+"]", e);
        }finally {
            // 데이터/읽기 영역 통제 해제
            dataSema.release();
            readSema.release(MAX_READER);
        }
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
            for(Long k: dataMap.keySet()) {
                keyList.add(k);
            }
            // 키 영역 정렬
            Collections.sort(keyList, (a, b)->a.compareTo(b));
            // 동기화 건수 보정
            if(diffCnt < 0) diffCnt = diffCnt*-1;
            // 최대 값 추출 - 트랜잭션 키 초기화 값 추출
            if(keyList.size()>0)maxKey = keyList.get(keyList.size()-1);

            logger.info("<큐 정보 동기화 : 성공> = 큐명:[{}], 초기 KEY 갯수:[{}], 초기 DATA 화갯수:[{}], 동기화 건수:[{}]", QUEUE_NAME, listSize, mapSize, diffCnt);
        }
        return maxKey;
    }

    @Override
    public void write(Map<String, Object> dataMap) throws QueueWriteException {
        if(ObjectUtils.isEmpty(dataMap)){
            return;
        }
        long transactionKey = getTransactionKey();
        FileQueueData fData = new FileQueueData(transactionKey, dataMap);
        write(fData);
    }

    @Override
    public void write(FileQueueData fData) throws QueueWriteException {
        if(ObjectUtils.isEmpty(fData)){
            return;
        }
        if(fData.getTransactionKey() <= 0){
            fData.setTransactionKey(getTransactionKey());
        }
        writeQueueData(Arrays.asList(fData));
    }

    @Override
    public void write(List<Map<String, Object>> dataList) throws QueueWriteException {
        long groupKey = 0;
        if(ObjectUtils.isEmpty(dataList)){
            return;
        }
        int size = dataList.size();
        List<FileQueueData> fileQueueDataList = new ArrayList<FileQueueData>(dataList.size());
        if(size>1)groupKey = getGroupKey();
        for(Map<String, Object> dataMap : dataList){
            FileQueueData fData = new FileQueueData(getTransactionKey(), dataMap);
            if(size>1)fData.setGroupTransactionKey(groupKey);
            fileQueueDataList.add(fData);
        }

        writeQueueData(fileQueueDataList);
    }

    @Override
    public void writeQueueData(List<FileQueueData> fileQueueDataList) throws QueueWriteException {
        if(ObjectUtils.isEmpty(fileQueueDataList)){
            return;
        }

        long groupKey = -1;
        // 개별 인덱스 키 체크 및 부여
        for(FileQueueData fData : fileQueueDataList){
            // 개별 인덱스 체크 및 부여
            if(fData.getTransactionKey() <= 0)fData.setTransactionKey(getTransactionKey());

            // 데이터가 1개 이상이며, 그룹키 지정이 안된 데이터가 있을 경우 1회에 한정하여 공통 그룹 키 채번
            if(fileQueueDataList.size()>1 && groupKey <0 && fData.getGroupTransactionKey()<=0) groupKey = getGroupKey();
        }
        // 공통 그룹 키 지정
        if(groupKey > 0){
            for(FileQueueData fData : fileQueueDataList){
                fData.setGroupTransactionKey(groupKey);
            }
        }

        boolean isReadLock = false;
        boolean isBulk = fileQueueDataList.size()>1?true:false;
        try {
            // 데이터 영역 통제
            dataSema.acquire();
            // 데이터가 1개 이상 일 경우 읽기 영역 통제 - READ COMMIT/ROLLBACK 통제(STORE 트랜잭션 이슈)
            if(isBulk){
                readSema.acquire(MAX_READER);
                isReadLock = true;
            }

            // 데이터 영역에 데이터 입력
            for (FileQueueData fData : fileQueueDataList){
                long innerKey = fData.getTransactionKey();
                dataMap.put(innerKey, fData.toString());
                dataKeyList.add(innerKey);
            }
            store.commit();
            addCount(fileQueueDataList.size(), INPUT_COUNT);
            lastInTime = System.currentTimeMillis();
            logger.debug("<큐 쓰기 : 성공> = 큐 정보:[{}], 쓰레드 명:[{}],데이타 갯수:[{}]", QUEUE_NAME, Thread.currentThread().getName(), fileQueueDataList.size());
        }catch (Exception e){
            // 벌크 처리 경우만 STORE 롤백 수행
            if(isBulk) {
                store.rollback();
            }
            throw new QueueWriteException("<큐 쓰기 커밋 : 실패> = 큐:["+QUEUE_NAME+"], 큐 롤백 수행:["+isBulk+"]", e);
        }finally {
            // 데이터 영역 통제 해제
            dataSema.release();
            // 벌크 처리 경우 - 읽기 영역 통제 해제
            if(isReadLock)readSema.release(MAX_READER);
        }
    }

    private long getTransactionKey() {
        synchronized (transactionIndex) {
            transactionIndex.compareAndSet(Long.MAX_VALUE, 1L);
            return transactionIndex.getAndIncrement();
        }
    }

    private long getGroupKey() {
        synchronized (groupIndex) {
            groupIndex.compareAndSet(Long.MAX_VALUE, 1L);
            return groupIndex.getAndIncrement();
        }
    }

    @Override
    public List<FileQueueData> read(String threadName) throws QueueReadException {

        List<FileQueueData> queueDataList = null;

        // 읽기 영역에 기존 데이터가 있는지 확인 후 진행
        queueDataList = readBufferMap.get(threadName);
        if(ObjectUtils.isNotEmpty(queueDataList)){
            logger.debug("<단건 (버퍼)읽기 : 성공> = 큐:[{}], 키 정보{}], 데이터 갯수:[{}]", QUEUE_NAME, threadName, queueDataList.size());
            return queueDataList;
        }

        try{
            // 데이터 영역 통제
            dataSema.acquire();

            // 데이터 영역 데이터 건수 체크
            if(dataMap.size()<1){
                logger.debug("<단건 읽기 : 무시> = 큐:[{}], 키 정보:[{}], 데이터 없음", QUEUE_NAME, threadName);
                return null;
            }

            // 데이터 영역 키<>데이터 동기화
            realignKey(dataKeyList, dataMap);

            //키와 데이터 추출
            Long key = dataKeyList.get(0);
            String data = dataMap.get(key);

            // 큐 데이터 생성
            FileQueueData fData = FileQueueData.fromString(data);

            // 큐 데이터가 정상일 경우만 읽기 버퍼 영역에 저장
            if(ObjectUtils.isNotEmpty(fData)){
                // 큐 데이터 리스트 생성
                queueDataList = Arrays.asList(fData);
                readBufferMap.put(threadName, queueDataList);
            }

            // 데이터 영역에서 제거
            dataKeyList.remove(key);
            dataMap.remove(key);

            store.commit();
        }catch (Exception e){
            throw new QueueReadException("<단건 읽기 : 실패> = 큐:["+QUEUE_NAME+"]", e);
        } finally{
            // 데이터 영역 통제 해제
            dataSema.release();
        }

        logger.debug("<단건 읽기 : 성공> = 큐 정보:[{}], 키 정보:[{}], 데이터 갯수:[{}]", QUEUE_NAME, threadName, queueDataList.size());
        return queueDataList;
    }

    @Override
    public List<FileQueueData> read(String threadName, int readCount) throws QueueReadException {
        List<FileQueueData> queueDataList = null;
        boolean isReadLock = false;

        // 읽기 영역에 기존 데이터가 있는지 확인 후 진행
        queueDataList = readBufferMap.get(threadName);
        if(ObjectUtils.isNotEmpty(queueDataList)){
            logger.debug("<대량 (버퍼)읽기 : 성공> = 큐 정보:[{}], 키 정보:[{}], 데이터 갯수:[{}]", QUEUE_NAME, threadName, queueDataList.size());
            return queueDataList;
        }

        try {
            // 데이터 영역 통제
            dataSema.acquire();

            // 데이터 영역 데이터 건수 취득
            int mapSize = dataMap.size();
            if(mapSize < 1){
                logger.debug("<대량 읽기 : 무시> = 큐 정보:[{}], 키 정보:[{}], 데이터 없음", QUEUE_NAME, threadName);
                return null;
            }

            // 데이터 영역 키<>데이터 동기화
            realignKey(dataKeyList, dataMap);

            // 실제 전달 제한 건수 추출
            int selectCount = readCount > mapSize ? mapSize:readCount;
            queueDataList = new ArrayList<FileQueueData>(selectCount);

            // 읽기 영역 통제 - READ COMMIT/ROLLBACK 통제(STORE 트랜잭션 이슈)
            readSema.acquire(MAX_READER);
            isReadLock = true;

            // 데이터 영역에서 데이터 삭제 및 추출
            for(int i =1; i<=selectCount; i++){
                long transKey = dataKeyList.remove(0);
                String data = dataMap.remove(transKey);

                FileQueueData fData = FileQueueData.fromString(data);
                queueDataList.add(fData);
            }

            // 읽기 영역에 데이터 저장
            readBufferMap.put(threadName, queueDataList);
            store.commit();

            logger.debug("<대량 읽기 : 성공> = 큐:[{}], 키 정보{}], 데이터 갯수:[{}]", QUEUE_NAME, threadName, queueDataList.size());
        }catch (Exception e){
            store.rollback();
            throw new QueueReadException("<대량 읽기 : 실패> = 큐:["+QUEUE_NAME+"], 롤백 수행", e);
        }finally {
            // 데이터 영역 통제 해제
            dataSema.release();
            // 벌크 처리 경우 - 읽기 영역 통제 해제
            if(isReadLock)readSema.release(MAX_READER);
        }
        return queueDataList;
    }

    @Override
    public List<String> readAll(){
        return new ArrayList<>(dataMap.values());
    }

    @Override
    public void removeOne() throws QueueReadException {
        boolean isReadLock = false;
        try {
            // 데이터 영역 통제
            dataSema.acquire();

            // 데이터 영역 데이터 건수 취득
            int mapSize = dataMap.size();
            if(mapSize < 1){
                logger.info("<큐 최신 데이터 제거 : 무시> = 큐 정보:[{}], 데이터 없음", QUEUE_NAME);
                return;
            }

            // 데이터 영역 키<>데이터 동기화
            realignKey(dataKeyList, dataMap);

            // 읽기 영역 통제 - READ COMMIT/ROLLBACK 통제(STORE 트랜잭션 이슈)
            readSema.acquire(MAX_READER);
            isReadLock = true;

            // 데이터 영역에서 1건 데이터 삭제 및 추출
            long transKey = dataKeyList.remove(0);
            String data = dataMap.remove(transKey);

            store.commit();
            logger.info("<큐 최신 데이터 제거 : 성공> = 큐:[{}], 데이터내용:[{}]", QUEUE_NAME, data);
        }catch (Exception e){
            store.rollback();
            throw new QueueReadException("<대량 읽기 : 실패> = 큐:["+QUEUE_NAME+"], 롤백 수행", e);
        }finally {
            // 데이터 영역 통제 해제
            dataSema.release();
            // 벌크 처리 경우 - 읽기 영역 통제 해제
            if(isReadLock)readSema.release(MAX_READER);
        }
    }

    @Override
    public void removeReadBufferOne(String threadName) throws QueueReadException {
        try {
            // 읽기 버퍼 영역 통제
            readSema.acquire();
            List<FileQueueData> fileQueueDataList = readBufferMap.remove(threadName);
            if(ObjectUtils.isEmpty(fileQueueDataList)){
                logger.warn("<읽기 버퍼 제거 : 무시> = 큐:[{}], 데이터 없음", QUEUE_NAME);
                return;
            }

            // 커밋
            store.commit();
            logger.info("<읽기 버퍼 제거 : 성공> = 큐:[{}], 데이터 갯수:[{}]", QUEUE_NAME, fileQueueDataList.size());
            for(FileQueueData queueData:fileQueueDataList){
                logger.info("삭제 데이터:{[]}", queueData.toString());
            }
            addCount(fileQueueDataList.size(), OUTPUT_COUNT);
            lastOutTime = System.currentTimeMillis();
        }catch (Exception e){
            throw new QueueReadException("<읽기 커밋 : 실패> = 큐:["+QUEUE_NAME+"], 롤백 수행", e);
        }finally {
            // 읽기 버퍼 영역 통제 해제
            readSema.release();
        }
    }

    @Override
    public void clearData() throws QueueReadException {
        try{
            // 데이터 영역 통제
            dataSema.acquire();
            // 읽기 영역 통제 - READ COMMIT/ROLLBACK 통제(STORE 트랜잭션 이슈)
            readSema.acquire(MAX_READER);
            // 데이터 클렌징
            dataMap.clear();
            // 데이터 키 클렌징
            dataKeyList.clear();
            // 저장
            store.commit();
        }catch (Exception e){
            throw new QueueReadException("<데이터 영역 클렌징 : 실패> = 큐:["+QUEUE_NAME+"]", e);
        }finally {
            // 데이터 영역 통제 해제
            dataSema.release();
            // 벌크 처리 경우 - 읽기 영역 통제 해제
            readSema.release(MAX_READER);
        }
    }

    @Override
    public void clearReadBuffer() throws QueueReadException {
        try{
            // 읽기 영역 통제 - READ COMMIT/ROLLBACK 통제(STORE 트랜잭션 이슈)
            readSema.acquire(MAX_READER);
            readBufferMap.clear();
        }catch (Exception e){
            throw new QueueReadException("<읽기 버퍼 클렌징 : 실패> = 큐:["+QUEUE_NAME+"]", e);
        }finally {
            // 벌크 처리 경우 - 읽기 영역 통제 해제
            readSema.release(MAX_READER);
        }
    }

    @Override
    public void readCommit(String threadName) throws QueueReadException {

        try {
            // 읽기 버퍼 영역 통제
            readSema.acquire();
            List<FileQueueData> fileQueueDataList = readBufferMap.remove(threadName);
            if(ObjectUtils.isEmpty(fileQueueDataList)){
                logger.warn("<읽기 커밋 : 무시> = 큐:[{}], 키 정보:[{}], 데이터 없음", QUEUE_NAME, threadName);
                return;
            }

            // 커밋
            store.commit();
            logger.debug("<읽기 커밋 : 성공> = 큐:[{}], 키 정보:[{}], 데이터 갯수:[{}]", QUEUE_NAME, threadName, fileQueueDataList.size());
            addCount(fileQueueDataList.size(), OUTPUT_COUNT);
            lastOutTime = System.currentTimeMillis();
        }catch (Exception e){
            throw new QueueReadException("<읽기 커밋 : 실패> = 큐:["+QUEUE_NAME+"], 롤백 수행", e);
        }finally {
            // 읽기 버퍼 영역 통제 해제
            readSema.release();
        }
    }

    public void readRollBack(String threadName){
        List<FileQueueData> fileQueueDataList = readBufferMap.get(threadName);
        if(ObjectUtils.isEmpty(fileQueueDataList)){
            logger.warn("<읽기 롤백 : 무시> = 큐 정보:[{}], 키 정보:[{}], 데이터 없음", QUEUE_NAME, threadName);
            return;
        }
        try {
            // 읽기/데이터 영역 통제
            dataSema.acquire();
            readSema.acquire(MAX_READER);
            for(FileQueueData fdata : fileQueueDataList){
                long transactionKey = fdata.getTransactionKey();
                dataMap.put(fdata.getTransactionKey(), fdata.toString());
                dataKeyList.add(transactionKey);
            }
            // 읽기 영역에서 데이터 삭제
            readBufferMap.remove(threadName);

            // 데이터 키 정렬
            Collections.sort(dataKeyList, (a, b)->a.compareTo(b));

            store.commit();
            logger.debug("<읽기 롤백 : 성공> = 큐:[{}], 키 정보:[{}], 데이터 갯수:[{}]", QUEUE_NAME, threadName, fileQueueDataList.size());
        }catch (Exception e){
            logger.error("<읽기 롤백 : 실패> = 큐:[{}], 에러 발생:[{}]", QUEUE_NAME, e.toString());
            if(logger.isDebugEnabled()){
                logger.error("<읽기 롤백 : 실패> = 에러 상세", e);
            }
        }finally {
            readSema.release(MAX_READER);
            dataSema.release();
        }
    }

    private void addCount(long count, AtomicLong atomicLong){
        synchronized (atomicLong) {
            long currentValue = atomicLong.get();
            long newValue = (currentValue + count > Long.MAX_VALUE) ? count : currentValue + count;
            atomicLong.set(newValue);
        }
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
    public long getLastOutTime(){
        return lastOutTime;
    }

    @Override
    public boolean isOk() {
        if((store == null || dataMap == null || store.isClosed() || dataMap.isClosed())){
            logger.error("{} 파일큐 스토어가 유효하지 않은 상태");
            return false;
        }
        if(readBufferMap == null || readBufferMap.isClosed()) {
            logger.error("{} 읽기 버퍼가 유효하지 않은 상태");
            return  false;
        }
        if(!Files.exists(Paths.get(QUEUE))){
            logger.error("{} 파일큐를 찾을 수 없음 - 삭제 됨");
            return false;
        }
        return true;
    }

    @Override
    public int getQueueSize() {
        return dataMap == null? 0 : dataMap.size();
    }

    // 설정 값이며, 별도 내부에서 해당 값으로 제한 하지 않음, 큐를 사용하는 프로세스에서 참조 하는 값으로 사용
    @Override
    public int getMaxSize() {
        return LIMIT_SIZE;
    }

    @Override
    public void close() {
        if(store != null && !store.isClosed()) {
            store.close();
        }
        if(dataKeyList != null) {
            dataKeyList.clear();
        }
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
