package com.queue.file.controller;

import com.queue.file.vo.FileQueueConfigVo;
import com.queue.file.vo.FileQueueCustomConfigVo;
import com.queue.file.vo.StoreInfo;
import org.h2.mvstore.MVStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.stream.Stream;

final class TestSupport {
    private TestSupport() {}

    static Path createTempFile(String prefix, String suffix) throws IOException {
        return Files.createTempFile(prefix, suffix);
    }

    static Path createTempDirectory(String prefix) throws IOException {
        return Files.createTempDirectory(prefix);
    }

    static void deleteIfExists(Path path) throws IOException {
        if (path != null) {
            Files.deleteIfExists(path);
        }
    }

    static void deleteDirectory(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // 무시
                }
            });
        }
    }

    static StoreInfo newStoreInfo(Path file) {
        return newStoreInfo(file, null);
    }

    static StoreInfo newStoreInfo(Path file, FileQueueCustomConfigVo customConfig) {
        FileQueueConfigVo config = new FileQueueConfigVo(file.getParent().toString(), file.getFileName().toString());
        if (customConfig != null) {
            config.setCustomConfig(customConfig);
        }
        StoreInfo storeInfo = new StoreInfo(config);
        storeInfo.setStore(new MVStore.Builder().open());
        storeInfo.setStoreOpenTime(LocalDateTime.now());
        return storeInfo;
    }
}
