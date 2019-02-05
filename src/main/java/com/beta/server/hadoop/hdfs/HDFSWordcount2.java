package com.beta.server.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

/**
 * 重构HDFS word count
 * Created by beta on 2019/2/5.
 */
public class HDFSWordcount2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI(PropertiesUtil.get(Constant.HDFS_URL));

        FileSystem fs = FileSystem.get(uri, conf, PropertiesUtil.get(Constant.HDFS_USER));
        FSDataInputStream fis = fs.open(new Path(PropertiesUtil.get(Constant.INPUT_PATH)));

        MyContext context = new MyContext();
        Class c = Class.forName(PropertiesUtil.get(Constant.MAPPER_CLASSNAME));
        Mapper mapper = (Mapper) c.newInstance();
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        String l = null;
        while ((l = reader.readLine()) != null) {
            mapper.map(l, context);
        }

        Map<Object, Object> map = context.getCacheMap();
        Set<Map.Entry<Object, Object>> entries = map.entrySet();
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(PropertiesUtil.get(Constant.OUTPUT_PATH)));
        for (Map.Entry entry : entries) {
            fsDataOutputStream.writeUTF(entry.getKey() + ":" + entry.getValue());
        }

        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        fs.close();
    }
}
