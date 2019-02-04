package com.beta.server.hadoop.hdfs;

/**
 * Created by beta on 2019/2/4.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author beta
 * 使用java api操作hdfs文件系统
 */
public class HDFSApp {

    public final  static  String HDFS_URI = "hdfs://hadoop000:8020";
    public final static String USERNAME = "hadoop";
    public Configuration conf;
    public FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //创建配置对象
        conf = new Configuration();
        //设置副本数为1
        conf.set("dfs.replication", "1");

        //获取操作hdfs客户端的对象
        fs = FileSystem.get(new URI(HDFS_URI), conf, USERNAME);
    }

    @After
    public void tearDown() throws IOException {
        fs.close();
    }

    //创建文件夹
    @Test
    public void first() throws IOException {
        boolean rs = fs.mkdirs(new Path("/hdfs"));
        System.out.println(rs);
    }

    //查看HDFS文件的内容
    @Test
    public void text() throws IOException {
        FSDataInputStream fsDataInputStream = fs.open(new Path("/t.txt"));
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
    }

    //创建文件并写入内容
    @Test
    public void create() throws IOException {
        FSDataOutputStream fos = fs.create(new Path("/hdfs/test.txt"));
        fos.writeUTF("hello world");
        fos.flush();
    }

    /**
     * 如果创建的是空的Configuration，那么默认加载jar包里面的配置
     */
    @Test
    public void testReplication() {
        String s = conf.get("dfs.replication");
        System.out.println(s);
    }

    //重命名
    @Test
    public void rename() throws IOException {
        boolean rs = fs.rename(new Path("/hdfs/test.txt"), new Path("/hdfs/a.txt"));
        System.out.println(rs);
    }

    //拷贝本地文件到hdfs
    @Test
    public void copyFromLocalFile() throws IOException {
        fs.copyFromLocalFile(new Path("/Users/beta/Downloads/QQ20190127-0.jpg"), new Path("/hdfs/QQ20190127-0.jpg"));
    }

    //拷贝大文件时 带进度显示
    @Test
    public void copyFromLocalFile2() throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/hdfs/mysql.tar.gz"), new Progressable() {
            //可以显示进度
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        InputStream is = new BufferedInputStream(new FileInputStream(new File("/Users/beta/Downloads/mysql-5.6.41-linux-glibc2.12-x86_64.tar.gz")));

        IOUtils.copyBytes(is, fsDataOutputStream, 1024);
    }

    //下载文件
    @Test
    public void copyToLocalFile() throws IOException {
        fs.copyToLocalFile(new Path("/hdfs/a.txt"), new Path("/Users/beta/Downloads/test.txt"));
    }

    //列出文件夹信息
    @Test
    public void listStatus() throws IOException {
        FileStatus[] files = fs.listStatus(new Path("/hdfs/"));

        for (FileStatus f : files) {
            System.out.println("组信息：" + f.getGroup());
            System.out.println("块大小：" + f.getBlockSize());
            System.out.println("副本数量：" + f.getReplication());
            System.out.println("大小：" + f.getLen());
            System.out.println("路径：" + f.getPath());
            System.out.println("---------------------");
        }
    }

    //递归显示目录下的文件
    @Test
    public void listFilesRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> lists = fs.listFiles(new Path("/hdfs"), true);
        while (lists.hasNext()) {
            LocatedFileStatus f = lists.next();
            System.out.println("组信息：" + f.getGroup());
            System.out.println("块大小：" + f.getBlockSize());
            System.out.println("副本数量：" + f.getReplication());
            System.out.println("大小：" + f.getLen());
            System.out.println("路径：" + f.getPath());
            System.out.println("---------------------");
        }
    }

    //查看文件块信息（有那几块，在哪些节点上面）
    @Test
    public void getBlocks() throws IOException {
        FileStatus fileStatus = fs.getFileStatus(new Path("/hdfs/mysql.tar.gz"));

        BlockLocation[] blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for (BlockLocation b : blocks) {
            for (String name : b.getNames()) {
                System.out.println(name + ":起始偏移量：" + b.getOffset() + ":" + b.getLength());
            }
        }
    }

    //删除文件
    @Test
    public void delete() throws IOException {
        boolean delete = fs.delete(new Path("/hdfs/a.txt"), true);
        System.out.println(delete);
    }
}
