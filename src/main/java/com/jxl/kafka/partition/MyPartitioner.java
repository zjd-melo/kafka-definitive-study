package com.jxl.kafka.partition;

/**
 * Created by zhujiadong on 2018/4/24 16:44
 */

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 *  key为空使用默认的partitioner round-robin
 *  key不为空对key hash得到partition的位置, key和partition的对应关系不会轻易的改变但是如果partition的数据增加了那么后面的key
 *  对应的partition就会落在别的地方(所以partition的个数在最初就应该定下来,不要随便改)
 *
 *  自定义partition
 */
public class MyPartitioner implements Partitioner{
    public int partition(String topic, Object key, byte[] KeyBytes, Object value, byte[] ValueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic("java_topic");
        int numPartitions = partitionInfos.size();
        if ((KeyBytes==null)||(! (key instanceof String))){
            throw new InvalidRecordException("we except all messages to have customer name as key");
        }
        if (((String)key).equals("melo")){
            return numPartitions; //melo always go to the last partition
        }

        //other partition will get hashed to the rest of the partitions
        return (Math.abs(Utils.murmur2(KeyBytes)) % (numPartitions - 1));
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
