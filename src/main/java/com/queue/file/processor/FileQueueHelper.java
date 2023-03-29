package com.queue.file.processor;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.queue.file.vo.FileQueueData;
import org.apache.commons.lang3.StringUtils;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class FileQueueHelper {
	
	private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	private final Gson gson = new Gson();
	
	private MVStore store = null;

	private boolean autoCommitMode = false;

	private boolean encryptMode = false;

	private boolean readOnlyMode = false;

	private boolean compressMode = false;

	private boolean restoreMode = false;

	private MVMap<Long, String> dataMap = null;
	
	private final List<Long> keyList = new ArrayList<Long>();
	
	private final AtomicLong atomicIndex = new AtomicLong(1);

	private final String QUEUE;
	private final String QUEUE_PATH;
	private final String QUEUE_NAME;
	private final String ENCRYPT_KEY ="ENCRYPT_KEY";
	
	
	private final int HINT_INDEX =0;
	private final int TIME_INDEX =1;
	private final int DATA_INDEX =2;
	private final String HINT_FIELD = "FROM_ENV";
	private final String DELIMITER = "1U2R3A4C5L6E7U8M9S0";
	
	private long openTime = 0;
	private long lastInTime = 0;
	private long lastOutTime = 0;
	
	private long inputCnt =0;
	private long outputCnt =0;


	private FileQueueHelper(Map<String, Object> config){

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

		if(config.get("autoCommitMode")!=null)this.autoCommitMode = true;
		if(config.get("encryptMode")!=null) this.encryptMode = true;
		if(config.get("readOnly")!=null) this.readOnlyMode = true;
		if(config.get("compress")!=null) this.compressMode = true;
	}

	// 헬퍼 생성자 - 파일큐 정보 필요
	public FileQueueHelper(String queue) {
		QUEUE = queue;
		int index = 0;
		if(StringUtils.isNotBlank(queue)){
			index = queue.lastIndexOf(File.separator);
		}
		QUEUE_PATH = index>0?queue.substring(0,index):"";
		QUEUE_NAME = index>0?queue.substring(index+1):"";
	}

	// 헬퍼 생성자 - 파일큐 위치 정보, 파일큐 이름 정보
	public FileQueueHelper(String queuePath, String queueName) {
		QUEUE = queuePath+(queuePath.endsWith(File.separator)?"":File.separator)+queueName;
		QUEUE_PATH = queuePath;
		QUEUE_NAME = queueName;
	}

	// 파일큐를 Locking 하면서 점유 한다.
	public synchronized boolean open() {
		// 키 목록 초기화
		keyList.clear();

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

		if(Files.exists(Paths.get(QUEUE))) restoreMode = true;

		try {
			HashMap<String, Object> configMap = new HashMap<String, Object>(5);
			configMap.put("fileName", QUEUE);
			if(autoCommitMode == false)configMap.put("autoCommitDelay", 0);
			if(encryptMode)configMap.put("encryptionKey", this.ENCRYPT_KEY);
			if(readOnlyMode)configMap.put("readOnly", 1);
			if(compressMode)configMap.put("compress", 1);

			// 파일큐 스토어 생성
			store = getQueue(configMap);

			// 생성 실패 시 FALSE - 추후 생성 실패 시에 대한 후처리 논의 필요 
			if(store == null || store.isClosed()) return false;

			// 파일큐 스토어의 특정 스키마 해쉬 오픈
			dataMap = store.openMap(QUEUE_NAME);
			openTime = System.currentTimeMillis();

			// 파일큐 데이터와 키목록 일치 체크와 동기화
			reStoreKey(dataMap);
		}catch(Exception e) {
			logger.error(QUEUE_PATH+" 오픈 중 에러 발생:"+e);
			return false;
		}
		
		return true;
	}
	
	// 파일큐 종료 - 점유 자원 릴리즈 및 키목록 초기화
	public synchronized void close() {
		if(store != null) store.close();
		if(keyList != null) keyList.clear();
	}
	
	// 데이터 입력
	public synchronized boolean putData(Object object) {
		if(object == null)return true;
		// 파일큐 데이터와 키목록 일치 체크와 동기화
		reStoreKey(dataMap);
		// 인덱싱을 위한 인덱스 채번
		long index = getIndex();
		String hint = extractHint(object);
		String data = gson.toJson(object);
		long now = System.currentTimeMillis();
		// 파일큐 저장
		dataMap.put(index,  hint+DELIMITER+now+DELIMITER+data);
		// 키목록 인덱스 정보 추가
		keyList.add(index);
		inputCnt++;
		lastInTime = now;
		return true;
	}
	
	// 큐에 데이터 리스트를 넣는다
	public synchronized int putDataList(List<Object> dataList) {
		// 파일큐 데이터와 키목록 일치 체크와 동기화
		reStoreKey(dataMap);
		
		int insertCnt = 0;
		long now = System.currentTimeMillis();
		for(Object object : dataList) {
			long index = getIndex();
			String hint = extractHint(object);
			String data = gson.toJson(object);
			dataMap.put(index, hint+DELIMITER+now+DELIMITER+data);
			keyList.add(index);
			insertCnt +=1;
		}
		store.commit();
		lastInTime = now;
		inputCnt+=insertCnt;
		return insertCnt;
	}
	
	// 큐의 첫번째 데이터를 가져온다
	public synchronized FileQueueData getData() {
		FileQueueData fData = null;
		if(dataMap.size()<=0)return null;
		reStoreKey(dataMap);
		
		String data = null;		
		Long key = keyList.remove(0);
        data = dataMap.remove(key);
        if(data == null)return null;
        String[] datas = data.split(DELIMITER);
        fData = new FileQueueData(datas[HINT_INDEX], datas[DATA_INDEX], datas[TIME_INDEX]);
        lastOutTime = System.currentTimeMillis();
        outputCnt +=1;
		return fData;
	}
	
	// 지정한 갯수 만큼 순차적 데이터 목록를 가져온다
	public synchronized List<FileQueueData> getDataList(int count) {
		List<FileQueueData> dataList = new ArrayList<FileQueueData>();
		
		if(dataMap.size()<=0)return dataList;
		reStoreKey(dataMap);
		
		int loopCnt = count > keyList.size() ? keyList.size(): count;
		FileQueueData fData = null;
		for(int i=0; i<loopCnt; i++) {
			Long key = keyList.remove(0);
			String data = dataMap.remove(key);
			String[] datas = data.split(DELIMITER);
			fData = new FileQueueData(datas[HINT_INDEX], datas[DATA_INDEX], datas[TIME_INDEX]);
			if(StringUtils.isNotBlank(data)) {
				dataList.add(fData);
				outputCnt+=1;
			}
		}
		store.commit();
		lastOutTime = System.currentTimeMillis();
		
		return dataList;
	}
	
	// 현재 큐 대기 데이터 갯수 정보를 조회
	public int getDataCount() {
		return dataMap.size();
	}
	
	
	// 불량 데이터 정밀 확인 - json 정상 전환 여부 확인
	public List<FileQueueData> closeCheckSum(){
		List<FileQueueData> dataList = new ArrayList<FileQueueData>();
		if(dataMap.size()<=0)return dataList;
		
		FileQueueData fData = null;
		for(Entry<Long, String> element: dataMap.entrySet()) {
			String data = element.getValue();
			try {
				String[] datas = data.split(DELIMITER);
				fData = new FileQueueData(datas[HINT_INDEX], datas[DATA_INDEX], datas[TIME_INDEX]);
				gson.fromJson(fData.getData(), JsonObject.class);				
			}catch(Exception e) {
				dataList.add(fData);
			}
		}

		return dataList;
	}
	
	// 특정 큐의 불량 데이터를 제거
	public synchronized List<String> removePoorData(){
		List<String> dataList = new ArrayList<String>();

		Iterator<Entry<Long, String>> iter =  dataMap.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Long, String> entry = iter.next();
			Long key = entry.getKey();
			String data = entry.getValue();
			try {
				gson.fromJson(data, JsonObject.class);				
			}catch(Exception e) {
				keyList.remove(key);
				iter.remove();
				dataList.add(data);
			}
		}
		store.commit();
		return dataList;
	}
	
	public synchronized int clear() {
		int count = dataMap.size();
		dataMap.clear();
		keyList.clear();
		store.commit();
		return count;
	}
	
	public boolean isOk() {
		return (store == null || store.isClosed() || dataMap == null)?false:true;
	}
	
	
	private MVStore getQueue(HashMap<String, Object> configMap) {
		if(store == null || store.isClosed()) {
			try {
				String configInfo = DataUtils.appendMap(new StringBuilder(), configMap).toString();
				logger.debug("파일 큐 설정 정보 : {}", configInfo);
				store = new MVStore.Builder().fromString(configInfo).open();
			}catch(Exception e) {
				logger.error("{} 파일 큐 오픈 중 에러 발생:{}", QUEUE, e);
				return null;
			}
		}
		return store;
	}
	
	private long getIndex() {
		long index = atomicIndex.getAndIncrement();
		if(index >= Long.MAX_VALUE) {
			index = 1;
			atomicIndex.set(index);
		}
		return index;
	}
	
	private void reStoreKey(MVMap<Long, String> map) {
		int mapSize = map.size();
		int listSize = keyList.size();
		int diffCnt = mapSize - listSize;
		if(diffCnt == 0)return;
		
		for(Long k: map.keySet()) {
			keyList.add(k);
		}
		Collections.sort(keyList, new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				return o1>o2?1:-1;
			}
		});
		if(diffCnt < 0) diffCnt = diffCnt*-1;
		logger.warn("KEY DATA:"+listSize+", QUEUE DATA:"+mapSize+", 데이터 동기화("+diffCnt+"건)로 진행");
	}
	
	public String extractHint(Object object) {
		String hint = "";
		Class<?> classObject = object.getClass();

		List<Field> fields = getAllFields(classObject);
		for(Field f : fields) {
			f.setAccessible(true);
			String name = f.getName();
			try {
				if(name.equals(HINT_FIELD)) {
					hint = f.get(object) == null?"": f.get(object).toString();
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return hint;
	}

	public synchronized void commit(String filequeueName){
		try {
			if(store!=null) {
				store.commit();
				logger.info(filequeueName+ "발송파일큐 commit");
			}
		}catch (Exception e){
			logger.error(e.toString());
		}
	}
	
	public Map<String, String> getSummaryMapData() {
		Map<String, String> summaryMap = new HashMap<String, String>();
		summaryMap.put("QUEUE_NAME", ""+getQUEUE_NAME());
		summaryMap.put("QUEUE_PATH", ""+getQUEUE_PATH());
		summaryMap.put("CREATE_TIME", ""+getOpenTime());
		summaryMap.put("INPUT", ""+getInputCnt());
		summaryMap.put("OUTPUT", ""+getOutputCnt());
		summaryMap.put("LAST_INPUT_TIME", ""+getLastInTime());
		summaryMap.put("LAST_OUTPUT_TIME", ""+getLastOutTime());
		return summaryMap;
	}

	private List<Field> getAllFields(Class clazz) {
		if (clazz == null) {
			return Collections.emptyList();
		}

		List<Field> result = new ArrayList<>(getAllFields(clazz.getSuperclass()));
		List<Field> filteredFields = Arrays.stream(clazz.getDeclaredFields()).collect(Collectors.toList());
		result.addAll(filteredFields);
		return result;
	}

	public String getQUEUE_NAME() { return QUEUE_NAME;	}
	
	public String getQUEUE_PATH() { return QUEUE_PATH;	}

	public long getOpenTime() { return openTime; }

	public long getLastInTime() { return lastInTime; }

	public long getLastOutTime() {	return lastOutTime;	}

	public long getInputCnt() { return inputCnt; }

	public long getOutputCnt() { return outputCnt;	}


	public static final class Builder {
		private final HashMap<String, Object> config;

		private Builder(HashMap<String, Object> config) {
			this.config = config;
		}

		public Builder() {
			this.config = new HashMap();
		}

		private FileQueueHelper.Builder set(String var1, Object var2) {
			this.config.put(var1, var2);
			return this;
		}


		public FileQueueHelper.Builder queue(String queue) {
			return this.set("queue", queue);
		}

		public FileQueueHelper.Builder path(String path) {
			return this.set("path", path);
		}

		public FileQueueHelper.Builder name(String name) {
			return this.set("name", name);
		}

		public FileQueueHelper.Builder autoCommit() {
			return this.set("autoCommitMode", 1);
		}

		public FileQueueHelper.Builder encryptMode() {
			return this.set("encryptMode", 1);
		}

		public FileQueueHelper.Builder readOnly() {
			return this.set("readOnly", 1);
		}

		public FileQueueHelper.Builder compress() {
			return this.set("compress", 1);
		}


		public FileQueueHelper create() {
			return new FileQueueHelper(this.config);
		}

	}
	
}
