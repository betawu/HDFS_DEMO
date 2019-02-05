package com.beta.server.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Created by beta on 2019/2/5.
 * 提交到本地运行
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception {

        //设置用户
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        //hdfs配置
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop000:8020");

        //创建job
        //不传conf默认为本地文件系统
        Job job = Job.getInstance(conf);

        //设置主类
        job.setJarByClass(WordCountApp.class);

        //设置自定义mapper和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置combiner
        //为mapper端的聚合操作
        job.setCombinerClass(WordCountReducer.class);

        //设置输入路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));

        //防止重复输出
        Path outputPath = new Path("/wordcount/output");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop000:8020"), conf, "hadoop");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        //设置输出路径
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交作业
        boolean rs = job.waitForCompletion(true);

        System.exit(rs ? 0 : -1);
    }

}
