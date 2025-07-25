package com.queue.file.controller;

import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

            // 1. Write Format 버전 확인
            int writeFormatVersion = getWriteFormatVersion(store);

            // 2. lastChunkId 점검
            int lastChunkId = getLastChunkIdFromFileStore(store);
            int threshold = (int) (MAX_CHUNK_ID * CHUNK_ID_THRESHOLD_RATIO);
            if (lastChunkId >= threshold) {
                String msg = String.format("lastChunkId [%d] approaching MAX_ID [%d].", lastChunkId, MAX_CHUNK_ID);
                log.warn("⚠ {}", msg);
                // 3이하 버전에서는 동작 이슈 발생 lastChunkId MAX 값 도달 시
                if(writeFormatVersion < 3 ){
                    throw new IllegalStateException(msg);
                }
            } else {
                log.info("✓ lastChunkId OK: {}", lastChunkId);
            }

            // 3. read-only 여부
            if (store.isReadOnly()) {
                log.info("✓ Store is opened in read-only mode.");
            }

            // 4. unsaved changes 확인 및 대응
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

            // 4. 헤더 정보 출력
            Map<String, Object> meta = store.getStoreHeader();
            for(Map.Entry<String, Object> entry : meta.entrySet()){
                log.info("[HEADER INFO] {} = {}", entry.getKey(), entry.getValue());
            }

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

    public static int getWriteFormatVersion(MVStore store) {
        Map<String, Object> header = store.getStoreHeader();
        return header.get("format")==null ? 3 : Integer.parseInt(header.get("format").toString());
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
