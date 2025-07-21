package com.queue.file.vo;

import java.io.Serializable;

public class FileQueueData implements Serializable {
	private static final long serialVersionUID = 1L;

	private String partition = "";
	private String tag = "";
	private String data;

	private Long groupTransactionKey;
	private Long transactionKey;
	private final long createTime;

	public FileQueueData(String data) {
		this(0, 0, data);
	}
	public FileQueueData(long transactionKey, long createTime) {
		this(transactionKey, 0, "", "", "", createTime);
	}
	public FileQueueData(long transactionKey, String data) {
		this(transactionKey, 0, "", "", data, System.currentTimeMillis());
	}
	public FileQueueData(long transactionKey, long groupTransactionKey, String data) {
		this(transactionKey, groupTransactionKey, "", "", data, System.currentTimeMillis());
	}
	public FileQueueData(String tag, String data) {
		this("", tag, data);
	}
	public FileQueueData(String partition, String tag, String data) {
		this(0, 0, partition, tag, data, System.currentTimeMillis());
	}
	public FileQueueData(long transactionKey, String partition, String tag, String data) {
		this(transactionKey, 0, partition, tag, data, System.currentTimeMillis());
	}
	public FileQueueData(String partition, String tag, String data, Long time) {
		this(0, 0, partition, tag, data, time);
	}

	public FileQueueData(long transactionKey, long groupTransactionKey, String partition, String tag, String data, Long time) {
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

}
