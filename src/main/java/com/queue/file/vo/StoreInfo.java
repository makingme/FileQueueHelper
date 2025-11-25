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

    // Legacy: 단일 파일 모드용 (하위 호환성)
    private MVStore store = null;

    // New: 파티션별 독립 파일 모드용
    private final Map<String, MVStore> partitionStores = new ConcurrentHashMap<>();
    private final Map<String, LocalDateTime> partitionStoreOpenTimes = new ConcurrentHashMap<>();

    private final FileQueueConfigVo CONFIG;
    private LocalDateTime storeOpenTime;

    // Legacy 단일 Store 접근 (하위 호환성)
    public MVStore getStore() { return store; }
    public void setStore(MVStore store) { this.store = store; }

    // 파티션별 Store 관리
    public MVStore getPartitionStore(String partitionName) {
        if (CONFIG.getCustomConfig() != null && CONFIG.getCustomConfig().isUseSeparatePartitionFiles()) {
            return partitionStores.get(partitionName);
        }
        // Legacy 모드: 모든 파티션이 동일 Store 사용
        return store;
    }

    public void setPartitionStore(String partitionName, MVStore partitionStore) {
        partitionStores.put(partitionName, partitionStore);
        partitionStoreOpenTimes.put(partitionName, LocalDateTime.now());
    }

    public Map<String, MVStore> getPartitionStores() {
        return Collections.unmodifiableMap(partitionStores);
    }

    public void closePartitionStore(String partitionName) {
        MVStore partitionStore = partitionStores.remove(partitionName);
        if (partitionStore != null && !partitionStore.isClosed()) {
            partitionStore.close();
        }
        partitionStoreOpenTimes.remove(partitionName);
    }

    public void closeAllPartitionStores() {
        for (Map.Entry<String, MVStore> entry : partitionStores.entrySet()) {
            MVStore partitionStore = entry.getValue();
            if (partitionStore != null && !partitionStore.isClosed()) {
                try {
                    partitionStore.close();
                } catch (Exception e) {
                    // 로깅은 호출자가 처리
                }
            }
        }
        partitionStores.clear();
        partitionStoreOpenTimes.clear();
    }

    public FileQueueConfigVo getCONFIG() { return CONFIG; }

    public LocalDateTime getStoreOpenTime() { return storeOpenTime; }
    public void setStoreOpenTime(LocalDateTime storeOpenTime) { this.storeOpenTime = storeOpenTime; }

    public LocalDateTime getPartitionStoreOpenTime(String partitionName) {
        return partitionStoreOpenTimes.get(partitionName);
    }
}
