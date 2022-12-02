package com.queue.file.vo;

public class FileQueueData {
	private String hint;
	private Long createTime;
	private String data;
	
	public FileQueueData(String hint, String data, String createTime) {
		this.hint = hint;
		this.data = data;
		this.createTime = Long.parseLong(createTime.replaceAll("\\D", "0"));
	}
		
	public String getHint() { return hint;	}
	
	public Long getCreateTime() { return createTime; }
	
	public String getData() { return data;	}
	
}
