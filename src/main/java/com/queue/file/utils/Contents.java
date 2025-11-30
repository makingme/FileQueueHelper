package com.queue.file.utils;

/**
 * v3.0.0: Partition concept removed
 * @since : 2025-07-11(금)
 */
public class Contents {
    public static final String READ_BUFFER_SUFFIX = "READ_BUFFER";
    public static final String DATA_MAP_SUFFIX = "DATA_MAP";
    public static final String CACHE_SUFFIX = "CACHE";

    public static String getReadBufferName(){
        return READ_BUFFER_SUFFIX;
    }

    public static String getDataMapName(){
        return DATA_MAP_SUFFIX;
    }

    public static String getCacheName(){
        return CACHE_SUFFIX;
    }
}
