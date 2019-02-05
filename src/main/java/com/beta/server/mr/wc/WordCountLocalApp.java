package com.beta.server.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by beta on 2019/2/5.
 * 使用本地文件系统
 */
public class WordCountLocalApp {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //创建job
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

        //设置输入路径
        FileInputFormat.setInputPaths(job, new Path("input/wordcount.txt"));

        //设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("output"));

        //提交作业
        boolean rs = job.waitForCompletion(true);

        System.exit(rs ? 0 : -1);
    }
}
