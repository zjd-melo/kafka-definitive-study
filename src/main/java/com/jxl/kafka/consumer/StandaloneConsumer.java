package com.jxl.kafka.consumer;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by zhujiadong on 2018/4/27 14:41
 */

public class StandaloneConsumer {
    /*
        之前的消费者都是基于消费者组的,这个例子是单独的消费者可以订阅一个topic或者一个topic的几个partitions,如果这样不要用subscribe()
        而是使用assign()来消费特定的partition
     */
    public void standalone(){
        Set<TopicPartition> partitionSet = new HashSet<TopicPartition>();
        List<PartitionInfo>  partitionInfos = null;
        partitionInfos = ConsumerTest.kafkaConsumer.partitionsFor("topic");
        if(partitionInfos!=null){
            for(PartitionInfo partitionInfo:partitionInfos){
                partitionSet.add(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()));
            }
            ConsumerTest.kafkaConsumer.assign(partitionSet);
            while (true){
                //略
            }
        }
    }
}
//如果有新的consumer加进来,这个consumer是不知道的,要定期调用consumer.partitionsFor()来检查是否有partition加进来了