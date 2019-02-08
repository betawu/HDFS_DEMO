package com.beta.server.hadoop.mr.project.mr;

import com.beta.server.hadoop.mr.project.utils.ContentUtil;
import com.beta.server.hadoop.mr.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.util.Map;

/**
 * Created by beta on 2019/2/7.
 */
public class PageIdStatApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(PageIdStatApp.class);
        job.setMapperClass(PageIdStatMapper.class);
        job.setReducerClass(PageIdStatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileSystem fs = FileSystem.get(conf);
        Path inPutPath = new Path("input/trackinfo_20130721.data");
        Path outPutPath = new Path("output/pageIdStat");
        if (fs.exists(outPutPath)) {
            fs.delete(outPutPath, true);
        }
        FileInputFormat.setInputPaths(job, inPutPath);
        FileOutputFormat.setOutputPath(job, outPutPath);

        boolean rs = job.waitForCompletion(true);
        System.exit(rs ? 0 : 1);
    }

    static class PageIdStatMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private LongWritable one = new LongWritable(1);
        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> map = logParser.parse(log);
            String url = map.get("url");
            if (StringUtils.isNotBlank(url)) {
                String pageId = ContentUtil.getPageId(url);
                context.write(new Text(pageId), one);
            }
        }
    }

    static class PageIdStatReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable i : values) {
                count++;
            }

            context.write(key, new LongWritable(count));
        }
    }
}
