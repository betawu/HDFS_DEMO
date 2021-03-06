package com.beta.server.hadoop.mr.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by beta on 2019/2/6.
 * 提交到yarn上去执行
 */
public class AccessYARNApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(AccessApp.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);
        job.setCombinerClass(AccessCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(Access.class);
        job.setOutputValueClass(NullWritable.class);

        //自定义分区规则
        job.setPartitionerClass(AccessPartitioner.class);

        //自定义分区个数
        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
