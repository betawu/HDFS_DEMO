package com.beta.server.hadoop.mr.access;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by beta on 2019/2/6.
 */
public class AccessReducer extends Reducer<Text, Access, Access, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        Iterator<Access> iterator = values.iterator();

        while (iterator.hasNext()) {
            Access next = iterator.next();
            up += next.getUp();
            down += next.getDown();
        }

        context.write(new Access(key.toString(), up, down), NullWritable.get());
    }
}
