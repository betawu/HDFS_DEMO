package com.beta.server.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by beta on 2019/2/6.
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    /**
     *
     * @param text  手机号
     * @param access 统计数据
     * @param i reducer task任务数量
     * @return
     */
    @Override
    public int getPartition(Text text, Access access, int i) {
        String phone = text.toString();
        if (phone.startsWith("13")) {
            return 0;
        }
        else if(phone.startsWith("15")) {
            return 1;
        }
        else {
            return 2;
        }
    }
}
