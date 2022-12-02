package com.queue.file.manager;

import com.google.gson.Gson;
import com.queue.file.processor.FileQueueHelper;
import com.queue.file.vo.FileQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	public synchronized boolean openQueue(String queuePath) {
		boolean isOk = false;
		FileQueueHelper helper = new FileQueueHelper(queuePath);
		isOk = helper.open();
		if(isOk)HELPER_MAP.put(queuePath, helper);
		return isOk;
	}
	
	private FileQueueHelper getFileQueue(String queuePath) {
		FileQueueHelper helper = HELPER_MAP.get(queuePath);
		if(helper != null && helper.isOk()) return helper;
		boolean isOk = false;
		try {
			helper = new FileQueueHelper(queuePath);
			isOk = helper.open();
			if(isOk) {
				HELPER_MAP.put(queuePath, helper);
			}else {
				helper.close();
				helper = null;
			}
		}catch(Exception e) {
			logger.error(queuePath+" 오픈 중 에러 발생:"+e);
		}
		
		return isOk?helper:null;
	}
	
	// 지정 큐를 닫는다.
	public synchronized void closeQueue(String queuePath) {
		FileQueueHelper helper = HELPER_MAP.remove(queuePath);
		if(helper != null) helper.close();
	}
	
	// 특정 큐에 데이터를 넣는다
	public synchronized boolean putData(String queuePath, Object object) {
		boolean isOk = false;
		FileQueueHelper helper = getFileQueue(queuePath);
		if(helper == null) return false;
		
		isOk = helper.putData(object);
		return isOk;
	}
	
	// 특정큐에 데이터 리스트를 넣는다
	public synchronized int putDataList(String queuePath, List<Object> dataList) {
		int insertCnt = 0;

		FileQueueHelper helper = getFileQueue(queuePath);
		if(helper == null) return -1;
		
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
