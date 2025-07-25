package com.queue.file.controller;

import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * @since : 2025-07-25(금)
 */
public class MVStoreHealthChecker {
    private static final Logger log = LoggerFactory.getLogger(MVStoreHealthChecker.class);
    private static final int MAX_CHUNK_ID = 67108863;
    private static final double CHUNK_ID_THRESHOLD_RATIO = 0.9;

    public static void check(String filePath) throws IllegalStateException{
        MVStore store = null;
        try {
            store = new MVStore.Builder()
                    .fileName(filePath)
                    .readOnly()
                    .open();

            // 1. FileStore.lastChunkId 점검
            int lastChunkId = getLastChunkIdFromFileStore(store);
            int threshold = (int) (MAX_CHUNK_ID * CHUNK_ID_THRESHOLD_RATIO);
            if (lastChunkId >= threshold) {
                String msg = String.format("lastChunkId [%d] approaching MAX_ID [%d].", lastChunkId, MAX_CHUNK_ID);
                log.warn("⚠ {}", msg);
                throw new IllegalStateException(msg);
            } else {
                log.info("✓ lastChunkId OK: {}", lastChunkId);
            }

            // 2. read-only 여부
            if (store.isReadOnly()) {
                log.info("✓ Store is opened in read-only mode.");
            }

            // 3. unsaved changes 확인 및 대응
            if (store.hasUnsavedChanges()) {
                log.warn("⚠ Store has unsaved changes. Consider rollback or recovery. Executing rollback...");
                store.rollback();
                if (store.hasUnsavedChanges()) {
                    throw new IllegalStateException("Rollback failed. Store remains dirty.");
                } else {
                    log.info("✓ Rollback completed. Store is now clean.");
                }
            } else {
                log.info("✓ Store has no unsaved changes.");
            }

            // 4. 메타맵 출력
            Map<String, String> meta = store.getMetaMap();
            log.info("Meta Info: format={}, chunk={}, creationTime={}",
                    meta.get("format"),
                    meta.get("chunk"),
                    meta.get("creationTime"));

        } catch (MVStoreException e) {
            log.error("❌ Failed to open MVStore. Possibly due to unsupported format or file corruption: {}", e.getMessage());
        } catch (Exception e) {
            log.error("❌ Unexpected error during MVStore health check", e);
        } finally {
            if (store != null) {
                try{
                    store.close();
                }catch (Exception ignore){}
            }
        }
    }

    /**
     * MVStore 내부의 FileStore 객체에서 lastChunkId 추출
     */
    private static int getLastChunkIdFromFileStore(MVStore store) {
        try {
            // 시도 1: H2 2.2.x 이후 버전 (FileStore.lastChunkId)
            Field fileStoreField = MVStore.class.getDeclaredField("fileStore");
            fileStoreField.setAccessible(true);
            Object fileStore = fileStoreField.get(store);

            try {
                Field lastChunkIdField = fileStore.getClass().getSuperclass()
                        .getSuperclass().getDeclaredField("lastChunkId");
                lastChunkIdField.setAccessible(true);
                return lastChunkIdField.getInt(fileStore);
            } catch (NoSuchFieldException e) {
                // 시도 2: 혹시 모를 older 버전 호환용
                log.warn("⚠ Field 'lastChunkId' not found in FileStore. Trying MVStore...");
            }

        } catch (Exception e) {
            log.warn("⚠ Failed to access FileStore from MVStore. Possibly older version. Trying MVStore directly...");
        }

        // 시도 3: MVStore.lastChunkId (1.4 ~ 2.1 시절)
        try {
            Field legacyField = MVStore.class.getDeclaredField("lastChunkId");
            legacyField.setAccessible(true);
            return legacyField.getInt(store);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to access lastChunkId. Unsupported or unknown MVStore structure.", e);
        }
    }
}
