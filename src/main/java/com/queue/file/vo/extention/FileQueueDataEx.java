package com.queue.file.vo.extention;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class FileQueueDataEx implements Serializable {
	public static final String DELIMITER = "1K2K3B4K5D6I7P8Y9N0";
	private final static Gson gson = new Gson();
	private String partition = "";
	private String tag = "";

	private Long groupTransactionKey;
	private Long transactionKey;
	private final long createTime;
	private String data;

	public FileQueueDataEx(String data) {
		this(0, 0, data);
	}
	public FileQueueDataEx(long transactionKey, long createTime) {
		this(transactionKey, 0, "", "", "", createTime);
	}
	public FileQueueDataEx(long transactionKey, String data) {
		this(transactionKey, 0, "", "", data, System.currentTimeMillis());
	}
	public FileQueueDataEx(long transactionKey, long groupTransactionKey, String data) {
		this(transactionKey, groupTransactionKey, "", "", data, System.currentTimeMillis());
	}
	public FileQueueDataEx(String tag, String data) {
		this("", tag, data);
	}
	public FileQueueDataEx(String partition, String tag, String data) {
		this(0, 0, partition, tag, data, System.currentTimeMillis());
	}
	public FileQueueDataEx(long transactionKey, String partition, String tag, String data) {
		this(transactionKey, 0, partition, tag, data, System.currentTimeMillis());
	}
	public FileQueueDataEx(String partition, String tag, String data, Long time) {
		this(0, 0, partition, tag, data, time);
	}
	public FileQueueDataEx(long transactionKey, long groupTransactionKey, String partition, String tag, String data, Long time) {
		this.transactionKey = transactionKey;
		this.groupTransactionKey = groupTransactionKey;
		this.partition = partition;
		this.tag = tag;
		this.data = data;
		if(time == null || time <= 0){
			this.createTime = System.currentTimeMillis();
		}else{
			this.createTime = time;
		}
	}

	public String getPartition() {return partition;}
	public void setPartition(String partition) { this.partition = partition; }

	public String getTag() { return tag;}
	public void setTag(String tag) { this.tag = tag; }

	public Long getTransactionKey() {return transactionKey;}
	public void setTransactionKey(Long transactionKey) {this.transactionKey = transactionKey;}

	public Long getGroupTransactionKey() {return groupTransactionKey;}
	public void setGroupTransactionKey(Long groupTransactionKey) {this.groupTransactionKey = groupTransactionKey;}

	public Long getCreateTime() { return createTime; }

	public String getData() {return data;}
	public void setData(String data) { this.data = data; }

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



	public static FileQueueDataEx fromString(String lowData){
		if(isValid(lowData) == false) return null;
		String data = lowData.substring(1, lowData.length()-1);

		return gson.fromJson(data, FileQueueDataEx.class);
	}

}
