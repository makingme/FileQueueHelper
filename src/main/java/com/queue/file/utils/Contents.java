package com.queue.file.utils;

/**
 * @since : 2025-07-11(금)
 */
public class Contents {
    public static final String DELIMITER ="___";
    public static final String DEFAULT_PARTITION = "DP201019";
    public static final String READ_BUFFER_SUFFIX = "READ_BUFFER";
    public static final String DATA_MAP_SUFFIX = "DATA_MAP";
    public static final String CACHE_SUFFIX = "CACHE";

    public static String getReadBufferName(){
        return getReadBufferName(DEFAULT_PARTITION);
    }

    public static String getReadBufferName(String partition){
        return partition + DELIMITER + READ_BUFFER_SUFFIX;
    }

    public static String getDataMapName(){
        return getDataMapName(DEFAULT_PARTITION);
    }

    public static String getDataMapName(String partition){
        return partition + DELIMITER + DATA_MAP_SUFFIX;
    }

    public static String getCacheName(){
        return getCacheName(DEFAULT_PARTITION);
    }

    public static String getCacheName(String partition){
        return partition + DELIMITER + CACHE_SUFFIX;
    }
}
