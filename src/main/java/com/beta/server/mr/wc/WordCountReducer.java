package com.beta.server.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by beta on 2019/2/5.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //map输出到reduce，按照相同的key分发给同一个reducer

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntWritable next = iterator.next();
            count += next.get();
        }

        context.write(key, new IntWritable(count));
    }
}
