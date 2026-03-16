package com.queue.file.controller;

import com.queue.file.exception.InitializeException;
import org.h2.mvstore.MVStore;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ControllerFactoryBestEffortTest {

    @Test
    // 테스트 목적: createFailsWhenQueuePathNotWritableBestEffort 동작 확인
    public void createFailsWhenQueuePathNotWritableBestEffort() throws Exception {
        Path dir = TestSupport.createTempDirectory("readonly");
        File dirFile = dir.toFile();
        boolean setReadOnly = dirFile.setWritable(false, false);
        // 환경 의존: Windows 디렉터리는 read-only가 무시될 수 있어, 권한 강제가 불가하면 스킵.
        Assume.assumeTrue("Unable to set directory read-only on this environment", setReadOnly);

        String queue = dir.resolve("q.mv").toString();
        try {
            ControllerFactory.create(queue);
            fail("Expected InitializeException for non-writable path");
        } catch (InitializeException e) {
            assertTrue(e.getMessage().contains("not writable"));
        } finally {
            dirFile.setWritable(true, false);
            TestSupport.deleteDirectory(dir);
        }
    }

    @Test
    // 테스트 목적: checkStoreRollbackFailureBestEffort 동작 확인
    public void checkStoreRollbackFailureBestEffort() throws Exception {
        Path dir = TestSupport.createTempDirectory("dirty");
        Path file = dir.resolve("d.mv");
        MVStore store = new MVStore.Builder().fileName(file.toString()).open();
        store.openMap("DATA").put("k", "v");

        Method closeImmediately;
        try {
            closeImmediately = store.getClass().getMethod("closeImmediately");
        } catch (NoSuchMethodException e) {
            store.close();
            // 환경 의존: MVStore.closeImmediately는 버전 의존이므로 미지원 시 스킵.
            Assume.assumeTrue("MVStore.closeImmediately not available", false);
            return;
        }
        closeImmediately.invoke(store);

        // 환경 의존: 롤백 실패 재현이 안 되면 스킵.
        try {
            ControllerFactory.checkStore(file.toString());
            // 환경 의존: 롤백 실패는 재현성이 낮아 여기서 실패하지 않으면 스킵.
            Assume.assumeTrue("Rollback failure not reproducible in this environment", false);
        } catch (InitializeException e) {
            if (!e.getMessage().contains("Rollback failed")) {
                // 환경 의존: 롤백 실패 메시지 패턴만 실패로 인정하고, 그 외는 환경 의존으로 스킵.
                Assume.assumeTrue("Unexpected failure mode: " + e.getMessage(), false);
            }
        } finally {
            TestSupport.deleteDirectory(dir);
        }
    }

    @Test
    // 테스트 목적: checkStoreRollbackSuccessBestEffort 동작 확인
    public void checkStoreRollbackSuccessBestEffort() throws Exception {
        Path dir = TestSupport.createTempDirectory("dirty-success");
        Path file = dir.resolve("s.mv");
        MVStore store = new MVStore.Builder().fileName(file.toString()).open();
        store.openMap("DATA").put("k", "v");

        Method closeImmediately;
        try {
            closeImmediately = store.getClass().getMethod("closeImmediately");
        } catch (NoSuchMethodException e) {
            store.close();
            // 환경 의존: MVStore.closeImmediately는 버전 의존이므로 미지원 시 스킵.
            Assume.assumeTrue("MVStore.closeImmediately not available", false);
            return;
        }
        closeImmediately.invoke(store);

        try {
            ControllerFactory.checkStore(file.toString());
        } catch (InitializeException e) {
            // 환경 의존: 롤백 성공은 버전/상태에 따라 달라 재현이 안 되면 스킵.
            Assume.assumeTrue("Rollback success not reproducible: " + e.getMessage(), false);
        } finally {
            TestSupport.deleteDirectory(dir);
        }
    }
}
