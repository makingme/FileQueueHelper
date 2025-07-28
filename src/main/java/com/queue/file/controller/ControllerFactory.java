package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import com.queue.file.vo.FileQueueConfigVo;
import com.queue.file.vo.FileQueueCustomConfigVo;
import com.queue.file.vo.StoreInfo;
import org.h2.mvstore.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * @since : 2025-07-21(월)
 * This Class generates BaseController Instance with Several Options(Mods)
 */
public class ControllerFactory {
    private static final Logger logger = LoggerFactory.getLogger(ControllerFactory.class);

    private static final int MAX_CHUNK_ID = 67108863;

    private static final double CHUNK_ID_THRESHOLD_RATIO = 0.9;
    /**
     * @param queue - Path+QueueName
     * @return BaseController Basic Mode Instance
     * @throws InitializeException - RuntimeException
     */
    public static BaseController create(String queue) throws InitializeException {
        logger.debug("create controller queue={}", queue);
        StoreInfo storeInfo = new StoreInfo(new FileQueueConfigVo(queue));
        initialize(storeInfo);
        return new BaseController(storeInfo);
    }

    /**
     * @param queuePath - exist Path
     * @param queueName - file queue name
     * @return BaseController Basic Mode Instance
     * @throws InitializeException - RuntimeException
     */
    public static BaseController create(String queuePath, String queueName) throws InitializeException {
        logger.debug("create controller path={} name={}", queuePath, queueName);
        StoreInfo storeInfo = new StoreInfo(new FileQueueConfigVo(queuePath, queueName));
        initialize(storeInfo);
        return new BaseController(storeInfo);
    }

    /**
     * @param queue - Path+QueueName
     * @return BaseController Stable Mode Instance
     * @throws InitializeException - RuntimeException
     */
    public static BaseController createStable(String queue) throws InitializeException {
        FileQueueConfigVo configVo = new FileQueueConfigVo(queue);
        FileQueueCustomConfigVo fileQueueCustomConfigVo = new FileQueueCustomConfigVo();
        fileQueueCustomConfigVo.setStableMode(true);
        configVo.setCustomConfig(fileQueueCustomConfigVo);
        return createCustomController(configVo);
    }

    /**
     * @param queue - Path+QueueName
     * @param bulkSize - Bulk Able Count
     * @return BaseController Bulk Commit Mode Instance
     * @throws InitializeException - RuntimeException
     */
    public static BaseController createBulk(String queue, int bulkSize) throws InitializeException {
        logger.debug("create bulk controller queue={} bulkSize={}", queue, bulkSize);
        FileQueueConfigVo configVo = new FileQueueConfigVo(queue);
        FileQueueCustomConfigVo fileQueueCustomConfigVo = new FileQueueCustomConfigVo();
        fileQueueCustomConfigVo.setBulkCommit(true);
        fileQueueCustomConfigVo.setBulkSize(bulkSize);
        configVo.setCustomConfig(fileQueueCustomConfigVo);
        return createCustomController(configVo);
    }

    /**
     * @param configVo - custom config
     * @return BaseController Custom Config Mode Instance
     * @throws InitializeException - RuntimeException
     */
    public static BaseController createCustomController(FileQueueConfigVo configVo) throws InitializeException {
        logger.debug("create custom controller with config={}", configVo.getQueue());
        StoreInfo storeInfo = new StoreInfo(configVo);
        initialize(storeInfo);
        return new BaseController(storeInfo);
    }

    private static void initialize(StoreInfo storeInfo) throws InitializeException {
        FileQueueConfigVo configVo = storeInfo.getCONFIG();
        logger.debug("openStore queue={}", configVo.getQueue());
        checkFile(storeInfo);
        checkStore(configVo.getQueue());
        if (storeInfo.getStore() == null || storeInfo.getStore().isClosed()) {
            HashMap<String, Object> configMap = new HashMap<>();
            configMap.put("fileName", configVo.getQueue());
            if (configVo.isReadOnlyMode()) configMap.put("readOnly", 1);
            if (configVo.isCompressMode()) configMap.put("compress", 1);

            FileQueueCustomConfigVo customConfigVo = configVo.getCustomConfig() == null ? new FileQueueCustomConfigVo() : configVo.getCustomConfig();
            configMap.put("autoCommitBufferSize", customConfigVo.getAutoCommitMemory());

            try {
                String configInfo = DataUtils.appendMap(new StringBuilder(), configMap).toString();
                MVStore.Builder builder = MVStore.Builder.fromString(configInfo);
                if (configVo.isEncryptMode()) builder.encryptionKey("123ENCRYPT_KEY321".toCharArray());
                MVStore store = builder.open();
                store.setAutoCommitDelay(customConfigVo.getAutoCommitDelay());
                store.setCacheSize(customConfigVo.getCacheSize());
                store.compact(1, 50);
                storeInfo.setStore(store);
                storeInfo.setStoreOpenTime(LocalDateTime.now());
            } catch (Exception e) {
                logger.error("openStore failed", e);
                throw new InitializeException("큐 오픈 실패: 큐=[" + configVo.getQueueName() + "], 에러=[" + e.getMessage() + "]", e);
            }
        }
    }

    private static void checkFile(StoreInfo storeInfo) throws InitializeException {
        logger.debug("validate queue={}", storeInfo.getCONFIG().getQueue());
        FileQueueConfigVo config = storeInfo.getCONFIG();
        String queue = config.getQueue();
        if (queue == null || queue.trim().isEmpty()) {
            throw new InitializeException("큐 활성화 실패: 큐 정보 누락 = 큐:[" + queue + "]");
        }
        String queueName = config.getQueueName();
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new InitializeException("큐 활성화 실패: 큐 이름 정보 누락 = 큐:[" + queue + "]");
        }
        String queuePath = config.getQueuePath();
        if (queuePath == null || queuePath.trim().isEmpty()) {
            throw new InitializeException("큐 활성화 실패: 큐 경로 정보 누락 = 큐:[" + queue + "]");
        }
        Path p = Paths.get(queuePath);
        if (!Files.exists(p)) {
            throw new InitializeException("큐 활성화 실패: 존재하지 않은 경로 = 경로 정보:[" + queuePath + "]");
        }

        if(!Files.isWritable(p)){
            throw new InitializeException("큐 활성화 실패: 지정 파일이 쓰기 가능 상태가 아님 = 경로 정보:[" + queuePath + "]");
        }

        if (Files.exists(Paths.get(queue))) {
            config.setRestoreMode(true);
        }
    }

    public static void checkStore(String filePath) throws InitializeException{
        MVStore store = null;
        try {
            store = new MVStore.Builder()
                    .fileName(filePath)
                    .open();

            // 1. Write Format 버전 확인
            int writeFormatVersion = getWriteFormatVersion(store);

            // 2. lastChunkId 점검
            int lastChunkId = getLastChunkIdFromFileStore(store);
            int threshold = (int) (MAX_CHUNK_ID * CHUNK_ID_THRESHOLD_RATIO);
            if (lastChunkId >= threshold) {
                String msg = String.format("lastChunkId [%d] approaching MAX_ID [%d].", lastChunkId, MAX_CHUNK_ID);
                logger.warn("⚠ {}", msg);
                // 3이하 버전에서는 동작 이슈 발생 lastChunkId MAX 값 도달 시
                if(writeFormatVersion < 3 ){
                    throw new InitializeException(msg);
                }
            } else {
                logger.info("✓ lastChunkId OK: {}", lastChunkId);
            }

            // 3. unsaved changes 확인 및 대응
            if (store.hasUnsavedChanges()) {
                logger.warn("⚠ Store has unsaved changes. Consider rollback or recovery. Executing rollback...");
                store.rollback();
                if (store.hasUnsavedChanges()) {
                    throw new InitializeException("Rollback failed. Store remains dirty.");
                } else {
                    logger.info("✓ Rollback completed. Store is now clean.");
                }
            } else {
                logger.info("✓ Store has no unsaved changes.");
            }

            // 4. 헤더 정보 출력
            Map<String, Object> meta = store.getStoreHeader();
            for(Map.Entry<String, Object> entry : meta.entrySet()){
                logger.info("[HEADER INFO] {} = {}", entry.getKey(), entry.getValue());
            }

        } catch (MVStoreException e) {
            logger.error("❌ Failed to open MVStore. Possibly due to unsupported format or file corruption: {}", e.getMessage());
        } catch (Exception e) {
            logger.error("❌ Unexpected error during MVStore health check", e);
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
    private static int getLastChunkIdFromFileStore(MVStore store) throws InitializeException{
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
                logger.warn("⚠ Field 'lastChunkId' not found in FileStore. Trying MVStore...");
            }

        } catch (Exception e) {
            logger.warn("⚠ Failed to access FileStore from MVStore. Possibly older version. Trying MVStore directly...");
        }

        // 시도 3: MVStore.lastChunkId (1.4 ~ 2.1 시절)
        try {
            Field legacyField = MVStore.class.getDeclaredField("lastChunkId");
            legacyField.setAccessible(true);
            return legacyField.getInt(store);
        } catch (Exception e) {
            throw new InitializeException("Unable to access lastChunkId. Unsupported or unknown MVStore structure.", e);
        }
    }

}
