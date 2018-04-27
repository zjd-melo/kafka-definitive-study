package com.jxl.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by zhujiadong on 2018/4/25 10:03
 */

//同一个线程中不能有多个同组的消费者同时多线程下也不能使用同一个consumer, one consumer per thread is the rule

public class ConsumerTest {
    /*
     * 发生rebalance时,consumer不可以进行消费,这个时间窗口应该很小,此外当一个partition被别的消费者消费时,消费者失去了当前的状态,如果它缓存
     * 了一些数据,还需要刷新缓存这会影响应用程序的性能
     *
     * kafka集群中有个broker被设计为group coordinator可以有多个来对应不同的消费者组,它通过心跳来管理消费者组中消费者之间的关系和他们消费
     * 的partition,只要消费者定期发送心跳它就会认为consumer是活的且能够从他的partition中处理消息,当消费者发送poll请求数据和commit records
     * it has consumed时发送心跳
     *
     * 如果一个消费者长时间没有发送heartbeats,session会timeout,group coordinator就认为挂了,此时触发rebalance
     * 如果一个消费者crashed并且停止处理消息,group coordinator会等待几秒来决定consumer是否挂了是否要触发rebalance,这几秒内该partition
     * 的消息不会被处理
     * 正常情况下是消费者通知group coordinator它要离开了,此时group coordinator立即触发rebalance,减少了等待的那几秒时间
     *
     * 新版本发生了变化,有单独的线程负责发送heartbeats,能够更频繁的发送减少触发rebalance的等待时间,对长时间处理消息的情况做了处理
     *
     * broker如何分配partition给consumers?
     * 当一个消费者想加入到一个组里面,它发送JoinGroup请求给group coordinator,第一个加入组的消费者成为group leader,这个leader从group
     * coordinator获取组中所有消费者(包括最近发送过心跳的consumer)的列表并且负责给其他consumer分配partition,它用PartitionAssignor
     * 的实现类来决定哪个partition归哪个consumer kafka有两种分配partition的策略,当分配结果出来后,group leader把结果列表给GroupCoordinator
     * GroupCoordinator把结果发给其他consumer,其他consumer只能看到自己被分配的partition,group leader是唯一一个知道所有分配结果(同一组中
     * 所有消费者和他们被分配的partition)的客户端进程,这个过程在发生rebalance时触发
     */
    public static Properties properties = new Properties();
    public static KafkaConsumer<String, String> kafkaConsumer = null;

    static {
        try {
            properties.load(ConsumerTest.class.getClassLoader().getResourceAsStream("kafkaconsumer.properties"));
            kafkaConsumer = new KafkaConsumer<String, String>(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void consumer(){
        kafkaConsumer.subscribe(Collections.singletonList("java_topic")); //可以有多个topic 可以是正则表达式(一旦一个新topic符合正则,rebalance立即触发)
//        kafkaConsumer.subscribe(Pattern.compile("test.*"));
        try{
            while (true){
                //consumer必须不断的poll msgs否则就会认为consumer挂了
                //poll不仅仅是取数据 如果是第一次消费(group leader)还要负责分配partition 发生rebalance在poll中处理分配partition的过程
                //还负责发生心跳(新版本不是这样),所有处理消息的过程应足够高效且快
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);//如果没有数据到consumer buffer 阻塞多久
                //应该在别的线程处理消息
                for(ConsumerRecord<String,String> record :consumerRecords){
                    System.out.println("topic="+record.topic()+" partition="+record.partition()+" offset="+record.offset()+" key="+record.key()+" value="+record.value());
                }
            }
        }finally {
            kafkaConsumer.close();
            //关闭连接,并且立即触发rebalance,而不是等着group coordinator发现consumer挂了才rebalance(会有一段间隔导致无法消费消息)
        }
    }

    public static void main(String[] args) {
        ConsumerTest consumerTest = new ConsumerTest();
        consumerTest.consumer();
    }
}
