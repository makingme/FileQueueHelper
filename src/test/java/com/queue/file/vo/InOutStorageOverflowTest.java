package com.queue.file.vo;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class InOutStorageOverflowTest {

    @Test
    // 테스트 목적: inputCountOverflowResetsToCount 동작 확인
    public void inputCountOverflowResetsToCount() throws Exception {
        InOutStorage storage = new InOutStorage();
        AtomicLong inputCount = getAtomicLong(storage, "inputCount");
        inputCount.set(Long.MAX_VALUE - 1);

        storage.addInputCount("producer", 10L);
        assertEquals(Long.valueOf(10L), storage.getInputCount());
    }

    private AtomicLong getAtomicLong(InOutStorage storage, String fieldName) throws Exception {
        Field field = InOutStorage.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (AtomicLong) field.get(storage);
    }
}
