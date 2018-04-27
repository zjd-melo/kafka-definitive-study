package com.jxl.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhujiadong on 2018/4/25 15:24
 */

/*
 * 无论什么时候调用poll()都会返回我们还没有读取到的消息,这就可以跟踪哪些msg已经被读取了,kafka不同于其他JMS queue的特性是它根本不跟踪consumer
 * 的acks,取而代之的是kafka允许消费者自己跟踪他们读取partition的位置 把这种消费端更新当前位置的行为叫做commit
 * 如何实现?consumer向kafka发送一条message到特殊的topic__consumer_offsets,保存每个partition的已经提交的offsets,如果发生rebalance
 * 新的消费者(指那些得到新partition的consumer)从这个topic中读取它分配到的partition对应的committed offsets,如果这个committed offset
 * 比最后处理的msg的offset(刚刚调用poll()取回的结果)小会导致重复消费,如果比它大会导致数据丢失
 *
 * 因此管理offset在客户端是一个很重要的问题 (KafkaConsumer API提供了多种方式提交commit)
 */
public class CommitsOffsets {
    /**
     * automatic commit
     *  最简单的方式是让consumer自己提交commit offsets(enable.auto.commit=true)默认每隔5秒(auto.commit.interval.ms)consumer会提
     *  交从poll()返回的最大的offset,就像发生在consumer端的其他事一样,automatic commits是由poll loop驱动的,无论什么时候你调用poll()
     *  consumer都会检查是不是满足自动提交的时间,如果是,就会提交poll()最近一次返回的最大的offsets
     *  这个会导致重复消费,比如在提交间隔的3秒后发生了rebalance,那么这3秒内的消息会重复消费,可以减少这个时间窗口但这样不能完全规避重复消费的情况
     *  而且它提交的总是之前的poll()返回的最大offsets并不能知道之前的消息有没有处理必须严格控制消息处理成功后再调用poll()
     *
     */

    /**
     * commit current offset
     * 大多数开发者需要更精确的控制commit offset来避免重复消费和消息丢失的问题,在某个点提交比基于时间的提交更有意义
     * 当auto.commit.offset=false时offset的提交必须明确的给出,最简单也是最可靠的提交APIcommitSync(),这个api总是提交当前poll()返回的
     * 最大offset并且在成功提交后立即返回,如果commit失败抛出异常,因为这个api总是提交当前poll()返回的最大offset所以确保在处理完所有消息后
     * 在调用这个方法,当触发rebalance时,最近一批的数据从开始到发生rebalance时会消费两次
     */
    private void commitCurrentOffset() {
        while (true) {
            ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic=" + record.topic() + " partition=" + record.partition() + " offset=" + record.offset() + " key=" + record.key() + " value=" + record.value());
            }
            try {
                //会产生阻塞
                ConsumerTest.kafkaConsumer.commitAsync();//处理完当前batch中的数据提交当前数据中最大的offset然后在获取新数据
            } catch (CommitFailedException e) { //当发生可恢复的错误时 这个方法会retry
                System.out.println("commit failed...");
            }
        }
    }

    /**
     * Asynchronous commit
     * 上面的同步提交offset的方法有个缺点,app会在等到broker响应提交结果时会阻塞,这影响了app的吞吐量,可以通过减少提交的频率来改善这点,但又会
     * 增加重复消费的问题当发生rebalance时
     * 异步提交不等待broker的响应结果继续poll()数据
     * 异步也有问题,当提交offset2000时发生网络问题,broker没有响应,此时offset 3000被成功提交,offset 2000在3000成功后重试,此时2000成功
     * 提交,这时发生rebalance时,会造成更多的duplicates
     * <p>
     * 解决方法是在异步提交时提供回调函数(不要在回调函数中重试不然和上面没什么区别)还可以用一个自增的序列来判断重试时有没有新的commit已经提交
     * 如果有就不重试 (没调用一次异步提交就把序列加上去retry时进行比对)
     */
    public void asynchronousCommit() {
        while (true) {
            ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic=" + record.topic() + " partition=" + record.partition() + " offset=" + record.offset() + " key=" + record.key() + " value=" + record.value());
            }
            //简单的异步提交  commit的顺序不能保证
            ConsumerTest.kafkaConsumer.commitAsync();//commit the last offset and carry on 也会重试
            //回调
            ConsumerTest.kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if (e != null) {
                        System.out.println("commit failed for offsets " + offsets); //可以打印日志或者发送到dashboard展示
                    }
                }
            });
        }
    }

    /**
     * combining synchronous and asynchronous commits
     * 其实偶尔的失败不是一个很大的问题,这个问题可能是占时的,以后的提交还是会成功的 但是在关闭consumer或者发生rebalance时要明确的给出最新的offset
     * 因此可以结合同步和异步来完成关闭consumer前的提交(发生rebalance时下面在说)
     */
    public void syncAndAsync() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic=" + record.topic() + " partition=" + record.partition() + " offset=" + record.offset() + " key=" + record.key() + " value=" + record.value());
                }

                ConsumerTest.kafkaConsumer.commitAsync();//没有错误发生时使用异步提交因为快 失败了 下一个提交会retry
            }
        } catch (Exception e) {
            System.out.println("Unexpectes error");
        } finally {
            try {
                ConsumerTest.kafkaConsumer.commitSync(); //当关闭consumer时,没有下一个commit, 所以使用同步提交,因为它会重试直到成功提交或者遇到不可重试错误时
            } finally {
                ConsumerTest.kafkaConsumer.close();
            }
        }
    }
    /**
     * commit specified offset
     * 提交最近的offset只允许你以完成batch处理的频率来提交,怎么能做到更快频率的提交呢？如果在处理过程中要提交offset以避免在长时间的处理过程
     * 中发生rebalance,commitSync()和commitAsync()都做不到,他们都是提交当批数据中最大的offsets
     * 幸运的是consumer API commitSync()和commitAsync()允许传一个map<partition, offset>
     */
    public void commitSpecifiedOffset(){
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();//map track offsets
        int count = 0;
        while (true){
            ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> record :records){
                System.out.printf("topic=%s,partition=%s,offset=%d,key=%s,value=%s\n", record.topic(),record.partition(),record.offset(),record.key(),record.value());
                currentOffsets.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset()+1,"no metadata"));
                if (count%1000==0){
                    ConsumerTest.kafkaConsumer.commitAsync(currentOffsets,null);
                }
                count++;
            }
        }
    }

}
