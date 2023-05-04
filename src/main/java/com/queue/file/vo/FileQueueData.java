package com.queue.file.vo;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Map;

public class FileQueueData implements Serializable {
	private final static Gson gson = new Gson();
	private String partition = "";
	private String tag = "";

	private long groupTransactionKey;
	private long transactionKey;
	private long createTime;
	private Map<String, Object> data;

	public FileQueueData(Map<String, Object> data) {
		this.data = data;
		this.createTime = System.currentTimeMillis();
	}

	public FileQueueData(long transactionKey, long createTime) {
		this.transactionKey = transactionKey;
		this.createTime = createTime;
	}

	public FileQueueData(long transactionKey, Map<String, Object> data) {
		this.transactionKey = transactionKey;
		this.data = data;
		this.createTime = System.currentTimeMillis();
	}

	public FileQueueData(String tag, Map<String, Object> data) {
		this.tag = tag;
		this.data = data;
		this.createTime = System.currentTimeMillis();
	}
	public FileQueueData(String partition, String tag, Map<String, Object> data) {
		this.partition = partition;
		this.tag = tag;
		this.data = data;
		this.createTime = System.currentTimeMillis();
	}

	public FileQueueData(String partition, String tag, Map<String, Object> data, String time) {
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

	public Long getTransactionKey() {return transactionKey;}
	public void setTransactionKey(Long transactionKey) {this.transactionKey = transactionKey;}

	public Long getGroupTransactionKey() {return groupTransactionKey;}

	public void setGroupTransactionKey(Long groupTransactionKey) {this.groupTransactionKey = groupTransactionKey;}

	public Long getCreateTime() { return createTime; }

	public Map<String, Object> getData() {return data;}


	public static boolean isValid(String lowData){

		if( StringUtils.isBlank(lowData) || lowData.startsWith("@{")== false || lowData.endsWith("}@") == false){
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("@").append(gson.toJson(this)).append("@").toString();
	}



	public static FileQueueData fromString(String lowData){
		if(isValid(lowData) == false) return null;
		String data = lowData.substring(1, lowData.length()-1);

		return gson.fromJson(data, FileQueueData.class);
	}

}
