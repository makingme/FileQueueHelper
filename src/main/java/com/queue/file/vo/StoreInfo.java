package com.queue.file.vo;

import org.h2.mvstore.MVStore;

import java.time.LocalDateTime;

/**
 * @since : 2025-07-07(월)
 */
public class StoreInfo {
    public StoreInfo(FileQueueConfigVo configVo) {
        this.CONFIG = configVo;
    }

    // 큐 데이터용 MVStore
    private MVStore store = null;

    // 캐시 데이터용 MVStore (v2.1.0)
    private MVStore cacheStore = null;

    private final FileQueueConfigVo CONFIG;
    private LocalDateTime storeOpenTime;
    private LocalDateTime cacheStoreOpenTime;

    // 큐 Store 접근
    public MVStore getStore() { return store; }
    public void setStore(MVStore store) {
        this.store = store;
        this.storeOpenTime = LocalDateTime.now();
    }

    // 캐시 Store 접근 (v2.1.0)
    public MVStore getCacheStore() { return cacheStore; }
    public void setCacheStore(MVStore cacheStore) {
        this.cacheStore = cacheStore;
        this.cacheStoreOpenTime = LocalDateTime.now();
    }

    // 모든 Store 닫기
    public void closeAll() {
        if (store != null && !store.isClosed()) {
            try {
                store.close();
            } catch (Exception e) {
                // 로깅은 호출자가 처리
            }
        }
        if (cacheStore != null && !cacheStore.isClosed()) {
            try {
                cacheStore.close();
            } catch (Exception e) {
                // 로깅은 호출자가 처리
            }
        }
    }

    public FileQueueConfigVo getCONFIG() { return CONFIG; }

    public LocalDateTime getStoreOpenTime() { return storeOpenTime; }
    public void setStoreOpenTime(LocalDateTime storeOpenTime) { this.storeOpenTime = storeOpenTime; }

    public LocalDateTime getCacheStoreOpenTime() { return cacheStoreOpenTime; }
    public void setCacheStoreOpenTime(LocalDateTime cacheStoreOpenTime) { this.cacheStoreOpenTime = cacheStoreOpenTime; }
}
