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

/**
 * @since : 2025-07-21(월)
 * This Class generates BaseController Instance with Several Options(Mods)
 */
public class ControllerFactory {
    private static final Logger logger = LoggerFactory.getLogger(ControllerFactory.class);

    public static final int MAX_ID = (Chunk.MAX_ID - 10000000);
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

        validate(storeInfo);
        int lastChunkId = getLastChunkId(configVo.getQueue());
        if(configVo.isRestoreMode() && lastChunkId >= MAX_ID) {
            logger.info("The lastChunkId has reached the internal limit({})", MAX_ID);
            MVStoreTool.compact(configVo.getQueue(), configVo.isCompressMode());
            logger.info("store is successfully compacted");
        }

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

    private static void validate(StoreInfo storeInfo) throws InitializeException {
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
        if (Files.exists(Paths.get(queue))) {
            config.setRestoreMode(true);
        }
    }

    private static int getLastChunkId(String path) {
        int lastChunkId = -1;
        MVStore store = null;
        try {
            store = new MVStore.Builder().
                    readOnly().
                    fileName(path)
                    .open();
            Field lastChunkIdField = MVStore.class.getDeclaredField("lastChunkId");
            lastChunkIdField.setAccessible(true);
            lastChunkId = lastChunkIdField.getInt(store);
            return lastChunkId;
        } catch (ReflectiveOperationException e) {
            throw new InitializeException("lastChunkId reflection failed", e);
        }catch (Exception e) {
            throw new RuntimeException("Failed to get lastChunkId via reflection", e);
        } finally {
            try {
                if(store != null &&  !store.isClosed()){
                    store.close();
                }
            }catch (Exception ignore){}
        }
    }
}
