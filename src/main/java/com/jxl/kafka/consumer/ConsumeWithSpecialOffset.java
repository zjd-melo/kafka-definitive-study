package com.jxl.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by zhujiadong on 2018/4/27 11:14
 */

//实现exactly-once 要确保offset被及时提交到外部存储系统
public class ConsumeWithSpecialOffset {
    /*
        consumer.seekToBeginning() consumer.seekToEnd()只消费新的消息
        从指定的位置开始消费 consumer.seek()
        如果不想丢失任何消息或者重复消费可以把offset写到其他数据库里
     */
    public void drawback(){
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        while (true){
            ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic=%s,partition=%s,offset=%d,key=%s,value=%s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));
//                processRecord(record);
//                storeRecordInDB(record);
                ConsumerTest.kafkaConsumer.commitAsync(currentOffsets,null);
            }
        }//这个例子每处理一个消息就提交一次offset 仍然有可能出错比如保存到数据库成功后突然crash那么这个offset没有提交,下次就会出现duplicates
        //保存数据到db和提交offset必须是原子操作要么同时成功要么同时失败当这样似乎不太可能
    }

    //如果在同一个事务中把offset和数据写到数据库中,那么当发生rebalance时,可以调用seek()到数据库中找到offset
    //从数据库中读取offset 配合seek()进行消费
    public void seek(){
        Map<TopicPartition,OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        ConsumerTest.kafkaConsumer.subscribe(Pattern.compile("java*"), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
//                commitDBTransaction();
            }

            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                for(TopicPartition partition:collection){
                    ConsumerTest.kafkaConsumer.seek(partition, getOffsetFromDB(partition));
                }
            }
        });

        ConsumerTest.kafkaConsumer.poll(0); //确保该消费者加入group 分配到partition
        //开始消费时读取offset poll从该offset获取数据 如果发生异常 poll抛出该异常
        for (TopicPartition partition:ConsumerTest.kafkaConsumer.assignment()){
            ConsumerTest.kafkaConsumer.seek(partition, getOffsetFromDB(partition));
        }
        while (true){
            ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic=%s,partition=%s,offset=%d,key=%s,value=%s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));
//                processRecord(record);
//                storeRecordInDB(record);  //假设存储记录很快
//            storeOffsetInDB(record.topic(),record.offset());
            }
//        commitDBTransaction();  //处理完一批数据提交事务 保证数据和offset都写到库中
        }
    }

    private long getOffsetFromDB(TopicPartition partition) {
        return 22222;
    }
}
