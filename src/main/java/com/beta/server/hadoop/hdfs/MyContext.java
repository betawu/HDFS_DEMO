package com.beta.server.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义上下文
 * Created by beta on 2019/2/5.
 */
public class MyContext {

    private Map<Object, Object> cacheMap = new HashMap<>();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    public void set(Object k, Object v) {
        cacheMap.put(k, v);
    }

    public Object get(Object k) {
        return cacheMap.get(k);
    }
}
