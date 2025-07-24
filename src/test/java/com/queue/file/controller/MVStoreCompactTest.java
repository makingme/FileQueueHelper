package com.queue.file.controller;

import com.queue.file.vo.FileQueueConfigVo;
import com.queue.file.vo.StoreInfo;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class MVStoreCompactTest {

    @Test
    public void testCompactTriggered() throws Exception {
        Path file = Files.createTempFile("compact", ".mv");
        String queue = file.toString();

        // Setup store with a high lastChunkId using reflection on FileStore
        MVStore store = new MVStore.Builder().fileName(queue).open();
        MVMap<String, String> map = store.openMap("d");
        map.put("k", "v");
        store.commit();

        Field fileStoreField = MVStore.class.getDeclaredField("fileStore");
        fileStoreField.setAccessible(true);
        Object fileStore = fileStoreField.get(store);
        Field lastChunkIdField = fileStore.getClass().getSuperclass()
                .getSuperclass().getDeclaredField("lastChunkId");
        lastChunkIdField.setAccessible(true);
        lastChunkIdField.setInt(fileStore, ControllerFactory.MAX_ID - 1);
        store.commit();
        store.close();

        // Initialize controller which should compact the store
        BaseController controller = ControllerFactory.create(queue);
        MVStore newStore = controller.getStoreInfo().getStore();
        Object newFileStore = fileStoreField.get(newStore);
        int newId = lastChunkIdField.getInt(newFileStore);
        assertTrue("compact should reset lastChunkId", newId < ControllerFactory.MAX_ID - 1);

        controller.close();
        Files.deleteIfExists(file);
    }
}
