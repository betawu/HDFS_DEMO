package com.beta.server.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * 获取配置参数工具类
 * Created by beta on 2019/2/5.
 */
public class PropertiesUtil {
    private static Properties prop = new Properties();

    static {
        try {
            prop.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("prop.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String get(String param) {
        return (String) prop.get(param);
    }
}
