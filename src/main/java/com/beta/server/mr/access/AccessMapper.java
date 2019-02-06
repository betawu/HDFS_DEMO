package com.beta.server.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by beta on 2019/2/6.
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split("\t");
        String phone = arr[1];
        long up = Long.parseLong(arr[arr.length - 3]);
        long down = Long.parseLong(arr[arr.length - 2]);

        Access access = new Access(phone, up, down);
        context.write(new Text(phone), access);
    }
}
