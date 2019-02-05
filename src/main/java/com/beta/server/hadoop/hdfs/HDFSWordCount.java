package com.beta.server.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;

/**
 * HDFS 词频统计
 * Created by beta on 2019/2/5.
 */
public class HDFSWordCount {
    public static void main(String[] args) throws Exception {
        //1.读取文件
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://hadoop000:8020");
        FileSystem fs = FileSystem.get(uri, conf, "hadoop");

        MyContext context = new MyContext();
        Mapper mapper = new MyMapper();
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/hdfs/wordcount"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus next = iterator.next();
            FSDataInputStream is = fs.open(next.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String l = null;
            while ((l = reader.readLine()) != null) {
                //2.业务处理
                mapper.map(l, context);
            }
        }

        //3.获取缓存
        Map<Object, Object> map = context.getCacheMap();
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/hdfs/wordcount/result.txt"));
        Set<Object> set = map.keySet();
        for (Object s : set) {
            fsDataOutputStream.writeUTF(s + ":" + map.get(s) + "\n");
        }

        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        fs.close();
    }
}
