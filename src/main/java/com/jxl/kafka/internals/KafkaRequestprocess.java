package com.jxl.kafka.internals;

/**
 * Created by zhujiadong on 2018/5/2 14:49
 */

public class KafkaRequestprocess {
    /*
        kafka broker干的最多的活就是处理发送给partition leader的请求(clients,partition replicas,controller),kafka在TCP之上自定义
        了二进制的协议用来处理请求和响应,kafka会顺序的处理接收到的请求
        所有的请求有标准的请求头:
            request type(API key)
            request version broker能够处理不同版本的clients的请求
            correlation ID 请求的唯一ID,会出现在响应中用来定位错误
            client ID 确定发送请求的客户端

        对于broker监听的每一个端口,broker都会运行acceptor线程用来创建连接并交给processor线程处理连接,processor threads(network
        threads)的数量是可以配置的,network threads负责从客户端的连接中取出请求把它放到request queue中,并且从response queue中收集
        响应并返回给客户端
        一旦把request放到request queue中,IO线程负责取出并处理他们,request的类型主要有两种
        produce requests
            send by producers and contains message the clients write to kafka brokers
        fetch requests
            send by consumers and follower replicas when they read messages from kafka brokers
        这两种请求必须发送给partition的leader,当给一个不存在leader partition的broker发送请求时会收到一个错误响应 Not a leader for
        partition kafka clients负责把相关的请求发送到包含leader的broker上
        那么客户端如何知道把请求发送给哪个broker呢,kafka clients 使用另外一种请求类型metadata request,包含客户端订阅的topic list和主题
        的分区信息,metadata request可以发送给任意节点,因为每个broker都有metadata的cache
        clients也会缓存这个信息来决定请求发给哪个broker上的leader partition,这个信息也需要定时刷新来决定metadata是否发生变化(
        metadata.max.age.ms)比如新的broker加入到集群,那么副本会发生变化,client会收到not a leader的error,说明cache过期了,重新发送
        metadata request

        produce request
        acks表示哪几个replica需要发送确认才认为这个消息是成功的写入到kafka,acks=1 just the leader acks=all all in-sync replicas
        acks=0 不需要收到任何确认信息
        当包含leader partition的broker收到一个produce request时,开始运行几步验证
            用户发送过来的数据是否在这个topic上有权限写
            request中的acks参数是否有效(0,1,all are allowed)
            如果acks是all,是否有足够的ISR能安全的写入(broker可以设置成拒绝新的message,如果ISR的数目小于了配置中的参数)
       然后才把消息写到本地磁盘,linux写到filesystem cache中(不保证何时写到磁盘),kafka不会等待数据写到磁盘而是依赖副本来保证消息的持久性
       一旦消息写到leader partition broker检查acks参数 如果是0或者1,broker立即返回,如果是all,request会保存到一个叫purgatory的buffer
       中直到leader partition观察到所有replicas都成功复制了这个消息才返回给客户端

       fetch request
       和produce request类似,发送给partition请求(哪个topic partition offset),同时指定每个partition返回数据的大小,不指定的话broker
       可能发送很多消息给client造成client OOM
       同样也要做metadata request决定leader partition的位置,当leader收到fetch请求,做一些校验比如offsets是否存在,如果客户端请求了一个很
       老的offsets这个offsets已经删除了,那么就会返回错误
       如果offsets存在,broker 从partition中读取消息直到达到客户端设置的上限,然后返回给client,kafka使用著名的zero-copy方法发送消息给
       客户端,也就是说kafka直接从file(linux filesystem cache)到network channel不需要任何中间层的过度,不像数据库先把数据cache到内存在
       返回,这个技术去掉了复制和管理内存buffer的过程,因此提高了性能
       客户端除了可以提高接收消息数据大小的上限,也可以减少这个限制,当topic中生产的消息不是很多时,可以设置成10K,也就告诉broker当你有10K数据时
       返回给我,而不是在很短的时间内多次请求,这样减少了cpu和网络的开销,当然也不是永远等待broker到达这个数据量才让他返回,可以指定超时时间,在这
       个时间内没有足够多的数据也放回给client处理,要注意的是,leader partition上不是所有的数据都可以被消费者消费的,当所有ISR都复制了这个消息
       才可以消费(HighWaterMark)保证了一致性,如果复制的速度慢也会影响消费者的性能,可以设置delay时间(replica.lag.time.max.ms)超过这个时
       间的新消息没有被同步,那么也认为这个消息是安全的(副本复制消息的最大允许延迟时间)

       上面的是kafka和client的通讯协议,kafka内部其实也是这么做的,但是client不应该使用这些kafka内部的协议
       这些协议也在变化,比如之前commitoffset是保存在zookeeper中的现在保存在kafka特殊的topic中,OffsetCommitRequest,
       OffsetFetchRequest等
     */
}
