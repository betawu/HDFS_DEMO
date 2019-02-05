package com.beta.server.hadoop.hdfs;

/**
 * 自定义Mapper接口
 * Created by beta on 2019/2/5.
 */
public interface Mapper {
    public void map(String line, MyContext context);
}
