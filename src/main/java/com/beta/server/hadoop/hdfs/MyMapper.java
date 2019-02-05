package com.beta.server.hadoop.hdfs;

/**
 * 自定义词频统计mapper
 * Created by beta on 2019/2/5.
 */
public class MyMapper implements Mapper {

    @Override
    public void map(String line, MyContext context) {
        String[] arr = line.split("\t");

        for (String s : arr) {
            if (context.get(s) == null) {
                context.set(s, 1);
            }
            else {
                context.set(s, Integer.parseInt(context.get(s).toString()) + 1);
            }
        }
    }
}
