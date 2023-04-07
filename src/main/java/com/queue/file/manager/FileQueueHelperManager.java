package com.queue.file.manager;

import com.google.gson.Gson;
import com.queue.file.processor.FileQueueHelper;
import com.queue.file.vo.FileQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class FileQueueHelperManager {
	private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	
	
	private final Map<String, FileQueueHelper> HELPER_MAP = new HashMap<String, FileQueueHelper>();
	
	private final static FileQueueHelperManager instance = new FileQueueHelperManager();
	
	private final Gson gson = new Gson();
	
	public static FileQueueHelperManager getInstance() {
		return instance;
	}
	
	public synchronized void enrollHelper(String key, FileQueueHelper helper) {
		HELPER_MAP.put(key, helper);
	}

	// 오토 커밋 모드:활성화, 읽기+쓰기모드:모두활성화 , 암호화모드:비활성화 , 압축모드:비활성화
	public synchronized boolean openQueue(String queue) {
		FileQueueHelper helper = new FileQueueHelper(queue);
		if(getFileQueue(helper.getQUEUE()) != null){
			logger.info("큐 생성 실패 - {} 큐가 존재 함", helper.getQUEUE());
			return false;
		}

		boolean isOk = helper.open();
		if(isOk)HELPER_MAP.put(helper.getQUEUE(), helper);
		return isOk;
	}

	// 오토 커밋 모드:활성화, 읽기+쓰기모드:모두활성화 , 암호화모드:비활성화 , 압축모드:비활성화
	public synchronized boolean openQueue(String queuePath, String queueName) {
		FileQueueHelper helper = new FileQueueHelper(queuePath, queueName);
		if(getFileQueue(helper.getQUEUE()) != null){
			logger.info("큐 생성 실패 - {} 큐가 존재 함", helper.getQUEUE());
			return false;
		}
		boolean isOk = helper.open();
		if(isOk)HELPER_MAP.put(helper.getQUEUE(), helper);
		return isOk;
	}

	// 오토 커밋 모드 지정, 읽기+쓰기 모드 지정, 암호화모드 지정, 압축모드 지정 가능
	public synchronized boolean openQueue(Map<String, Object> config) {
		FileQueueHelper helper = new FileQueueHelper(config);
		if(getFileQueue(helper.getQUEUE()) != null){
			logger.info("큐 생성 실패 - {} 큐가 존재 함", helper.getQUEUE());
			return false;
		}
		boolean isOk = helper.open();
		if(isOk)HELPER_MAP.put(helper.getQUEUE(), helper);
		return isOk;
	}

	
	private FileQueueHelper getFileQueue(String queuePath) {
		FileQueueHelper helper = HELPER_MAP.get(queuePath);
		if(helper != null && helper.isOk()) return helper;
		return null;
	}
	
	// 지정 큐를 닫는다.
	public synchronized void closeQueue(String queue) {
		FileQueueHelper helper = HELPER_MAP.remove(queue);
		if(helper != null) helper.close();
	}
	
	// 특정 큐에 데이터를 넣는다
	public synchronized boolean putData(String queue, Object object) {
		FileQueueHelper helper = getFileQueue(queue);
		if(helper == null) {
			logger.error("{} 지정한 큐가 존재 하지 않음 - 데이터 입력 실패", queue);
			return false;
		}

		return helper.putData(object);
	}
	
	// 특정큐에 데이터 리스트를 넣는다
	public synchronized int putDataList(String queue, List<Object> dataList) {
		int insertCnt = 0;

		FileQueueHelper helper = getFileQueue(queue);
		if(helper == null) {
			logger.error("{} 지정한 큐가 존재 하지 않음 - 데이터 입력 실패", queue);
			return -1;
		}
		
		insertCnt = helper.putDataList(dataList);
		
		return insertCnt;
	}
	
	// 특정큐의 첫번째 데이터를 가져온다
	public synchronized FileQueueData getData(String queuePath) {
		FileQueueData data = null;
		
		FileQueueHelper helper = getFileQueue(queuePath);
		if(helper == null) return null;
		
		data = helper.getData();
		return data;
	}
	
	// 특정큐의 지정 갯수의 순차적 데이터를 가져온다
	public synchronized List<FileQueueData> getDataList(String queuePath, int count) {
		List<FileQueueData> dataList = new ArrayList<FileQueueData>();
		
		FileQueueHelper helper = getFileQueue(queuePath);
		if(helper == null) return null;
		
		dataList = helper.getDataList(count);

		return dataList;
	}
		
	// 모든 큐의 누적 데이터 갯수 정보를 조회
	public int getAllDataCount() {
		int totalCnt =0;
		for(Entry<String, FileQueueHelper> entry : HELPER_MAP.entrySet()) {
			FileQueueHelper h = entry.getValue();
			totalCnt += h.getDataCount();
		}
		return totalCnt;
	}
	
	// 큐별 데이터 갯수 정보를 JSON 규격으로 전달
	public String getAllDataCountDetail() {
		Map<String, Map<String, String>> dataMap = new HashMap<String, Map<String, String>>(HELPER_MAP.size());
		for(Entry<String, FileQueueHelper> entry : HELPER_MAP.entrySet()) {
			FileQueueHelper helper = entry.getValue();
			dataMap.put(helper.getQUEUE_NAME(), helper.getSummaryMapData());
		}
		return gson.toJson(dataMap);
	}
	
	// 큐별 데이터 갯수 정보를 JSON 규격으로 전달
	public Map<String, Map<String, String>> getAllDataDetail() {
		Map<String, Map<String, String>> summaryMap = new HashMap<String, Map<String, String>>(HELPER_MAP.size());
		for(Entry<String, FileQueueHelper> entry : HELPER_MAP.entrySet()) {
			FileQueueHelper helper = entry.getValue();
			summaryMap.put(helper.getQUEUE_NAME(), helper.getSummaryMapData());
		}
		return summaryMap;
	}
	
	// 특정 큐의 데이터 갯수 조회
	public int getDataCount(String queuePath) {
		int dataCnt =0;
		
		FileQueueHelper helper = getFileQueue(queuePath);
		if(helper == null) return -1;	
		
		dataCnt = helper.getDataCount();
		
		return dataCnt;
	}
	
	// 특정 큐의 불량 데이터 정밀 확인 - json 정상 전환 여부 확인
	public List<FileQueueData> closeCheckSum(String queuePath){
		FileQueueHelper helper = getFileQueue(queuePath);
		if(helper == null) return null;
	
		return helper.closeCheckSum();	
	}
	
	// 특정 큐의 불량 데이터를 제거
	public List<String> removePoorData(String queuePath){
		FileQueueHelper helper = getFileQueue(queuePath);
		if(helper == null) return null;
		
		return helper.removePoorData();
	}
	
	// 특정 큐의 데이터 모두 지우기
	public int clearQueue(String queuePath) {
		FileQueueHelper helper = getFileQueue(queuePath);
		if(helper == null) return -1;
		
		return helper.clear();
	}

}
