package com.queue.file.vo;

import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * @since : 2025-07-07(월)
 */
public class StoreInfo {
    private static final Logger logger = LoggerFactory.getLogger(StoreInfo.class);

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

    /**
     * Closes all stores (main queue store and cache store).
     * v2.1.0: Now propagates exceptions to caller for proper error handling
     *
     * @throws Exception if any store fails to close
     */
    public void closeAll() throws Exception {
        Exception firstException = null;

        if (store != null && !store.isClosed()) {
            try {
                store.close();
            } catch (Exception e) {
                logger.error("Failed to close main store", e);
                firstException = e;
            }
        }

        if (cacheStore != null && !cacheStore.isClosed()) {
            try {
                cacheStore.close();
            } catch (Exception e) {
                logger.error("Failed to close cache store", e);
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    public FileQueueConfigVo getCONFIG() { return CONFIG; }

    public LocalDateTime getStoreOpenTime() { return storeOpenTime; }
    public void setStoreOpenTime(LocalDateTime storeOpenTime) { this.storeOpenTime = storeOpenTime; }

    public LocalDateTime getCacheStoreOpenTime() { return cacheStoreOpenTime; }
    public void setCacheStoreOpenTime(LocalDateTime cacheStoreOpenTime) { this.cacheStoreOpenTime = cacheStoreOpenTime; }
}
