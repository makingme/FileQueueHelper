package com.queue.file.vo;

import java.io.File;

/**
 * @since : 2025-07-08(화)
 */
public class FileQueueConfigVo {

    public FileQueueConfigVo(String queue) {
        int index = 0;
        if(queue!=null && !queue.trim().isEmpty()){
            index = queue.lastIndexOf(File.separator);
        }
        // 경로
        this.queuePath = index>0?queue.substring(0,index):"";
        // 파일 명
        this.queueName = index>0?queue.substring(index+1):"";

        this.queue = queue;
    }

    public FileQueueConfigVo(String queuePath, String queueName) {
        this.queuePath = queuePath;
        this.queueName = queueName;
        this.queue = queuePath+(queuePath.endsWith(File.separator)?"":File.separator)+queueName;
    }

    private final String queuePath;
    private final String queueName;
    private final String queue;

    private boolean encryptMode = false;
    private boolean compressMode = true;
    private boolean readOnlyMode = false;
    private boolean restoreMode = false;

    private int limit = 0;

    private FileQueueCustomConfigVo customConfig = new FileQueueCustomConfigVo();

    public String getQueue() { return queue; }
    public String getQueuePath() { return queuePath; }
    public String getQueueName() { return queueName; }

    public boolean isEncryptMode() { return encryptMode; }
    public void setEncryptMode(boolean encryptMode) { this.encryptMode = encryptMode; }

    public boolean isCompressMode() { return compressMode; }
    public void setCompressMode(boolean compressMode) { this.compressMode = compressMode; }

    public boolean isReadOnlyMode() { return readOnlyMode; }
    public void setReadOnlyMode(boolean readOnlyMode) { this.readOnlyMode = readOnlyMode; }

    public boolean isRestoreMode() { return restoreMode; }
    public void setRestoreMode(boolean restoreMode) { this.restoreMode = restoreMode; }

    public int getLimit() { return limit; }
    public void setLimit(int limit) { this.limit = limit; }

    public FileQueueCustomConfigVo getCustomConfig() { return customConfig; }
    public void setCustomConfig(FileQueueCustomConfigVo customConfig) { this.customConfig = customConfig; }
}
