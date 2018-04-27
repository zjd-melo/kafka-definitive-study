package com.jxl.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by zhujiadong on 2018/4/27 10:31
 */

public class RebalanceListener {
    /*
        在关闭consumer或者发生rebalance时,consumer会做一些清理工作(commit offset close file handles database connection)
        consumer API允许你运行自己的代码当consumer所消费的partition移出或加入时(rebalance)
        在调用subscribe()时传入ConsumerRebalanceListener实现类即可
        public void onPartitionsRevoked(Collection<TopicPartition> partitions)
            在发生rebalance开始时和consumer停止消费之前调用,在这里提交offsets所以无论哪个消费者接管了这个partition都能够知道从哪开始消费
        public void onPartitionsAssigned(Collection<TopicPartition> partitions)
            在重新分配partition之后和开始消费之前调用
     */
    public void partitionsRevoked(){
        final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        try{
            ConsumerTest.kafkaConsumer.subscribe(Pattern.compile("java*"), new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    System.out.println("Lost partitions in rebalance. Committing current offsets: "+currentOffsets);
                    ConsumerTest.kafkaConsumer.commitSync(currentOffsets);
                }

                public void onPartitionsAssigned(Collection<TopicPartition> collection) {

                }
            });
            while (true) {
                ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic=%s,partition=%s,offset=%d,key=%s,value=%s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                }
                ConsumerTest.kafkaConsumer.commitAsync(currentOffsets, null);
            }
        }catch (WakeupException e){
            //ignore, going closing
        }catch (Exception e){
            System.out.println("Unexpected error");
        }finally {
            try{
                ConsumerTest.kafkaConsumer.commitSync(currentOffsets);
            }finally {
                ConsumerTest.kafkaConsumer.close();
                System.out.println("Closed consumer and we are done");
            }
        }
    }
}
