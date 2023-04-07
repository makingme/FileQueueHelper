package com.queue.file.vo;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileQueueData {
	public final static String DELIMITER = "+1U2R3A4C9/2E7U8M9S0+";
	private String partition = "";
	private String tag = "";
	private Long key;
	private Long createTime;
	private String data;

	public FileQueueData(String data) {
		this.data = data;
		this.createTime = System.currentTimeMillis();
	}

	public FileQueueData(String tag, String data) {
		this.tag = tag;
		this.data = data;
		this.createTime = System.currentTimeMillis();
	}
	public FileQueueData(String partition, String tag, String data) {
		this.partition = partition;
		this.tag = tag;
		this.data = data;
		this.createTime = System.currentTimeMillis();
	}

	public FileQueueData(String partition, String tag, String data, String time) {
		this.partition = partition;
		this.tag = tag;
		this.data = data;
		if(StringUtils.isBlank(time)){
			this.createTime = System.currentTimeMillis();
		}else{
			this.createTime = Long.parseLong(time.replaceAll("\\D", "0"));
		}
	}

	public String getPartition() {return partition;}

	public String getTag() { return tag;}

	public Long getKey() {return key;}

	public Long getCreateTime() { return createTime; }
	
	public String getData() { return data;	}

	@Override
	public String toString() {
		return "@"+partition+DELIMITER+tag+DELIMITER+key+DELIMITER+createTime+DELIMITER+data+"@";
	}

	public static FileQueueData fromString(String lowData){
		if(StringUtils.isBlank(lowData) || isValid(lowData) == false){
			return null;
		}
		String[] dataArray = lowData.split(DELIMITER);
		return new FileQueueData(dataArray[0], dataArray[1], dataArray[3], dataArray[4]);
	}

	public static boolean isValid(String lowData){
		if(lowData.startsWith("@")== false || lowData.endsWith("@") == false){
			return false;
		}

		String[] dataArray = lowData.split(DELIMITER);
		if(dataArray.length != 5){
			return false;
		}
		return true;
	}
}
