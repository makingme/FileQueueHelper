package com.queue.file.vo;

import org.h2.mvstore.MVStore;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @since : 2025-07-07(월)
 */
public class StoreInfo {
    public StoreInfo(FileQueueConfigVo configVo) {
        this.CONFIG = configVo;
    }

    private MVStore store = null;
    private final FileQueueConfigVo CONFIG;

    private LocalDateTime storeOpenTime;

    public MVStore getStore() { return store; }
    public void setStore(MVStore store) { this.store = store; }

    public FileQueueConfigVo getCONFIG() { return CONFIG; }

    public LocalDateTime getStoreOpenTime() { return storeOpenTime; }
    public void setStoreOpenTime(LocalDateTime storeOpenTime) { this.storeOpenTime = storeOpenTime; }

}
