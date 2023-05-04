package com.queue.file.vo;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVStore;

import java.util.HashMap;

public class FileQueueBuilder {

    private final HashMap<String, Object> config;

    private FileQueueBuilder(HashMap<String, Object> config) {
        this.config = config;
    }

    public FileQueueBuilder() {
        this.config = new HashMap();
    }

    private FileQueueBuilder set(String key, Object value) {
        this.config.put(key, value);
        return this;
    }

    public FileQueueBuilder fileName(String var1) {
        return this.set("queue", var1);
    }
    public FileQueueBuilder path(String var1) {
        return this.set("path", var1);
    }

    public FileQueueBuilder name(String var1) {
        return this.set("name", var1);
    }

    public FileQueueBuilder autoCommitDisabled() {  return this.set("manualCommitMode", 0); }

    public FileQueueBuilder encrypt() {
        return this.set("encryptMode", 1);
    }

    public FileQueueBuilder readOnly() {
        return this.set("readOnly", 1);
    }

    public FileQueueBuilder compress() {
        return this.set("compress", 1);
    }
    public FileQueueBuilder limit(int var) {
        return this.set("limit", var);
    }

    public FileQueueBuilder multiRead(int var) {
        return this.set("multiRead", var);
    }

    public HashMap<String, Object> getConfig() {return config;}
}
