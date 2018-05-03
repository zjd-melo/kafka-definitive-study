package com.jxl.kafka.internals;

/**
 * Created by zhujiadong on 2018/5/3 11:34
 */

public class KafkaPhysicalStorage {
    /*
        kafka最基本的存储单位是partition,同一个partition不能存储在多个broker上甚至单台机器的不同的磁盘上,所以parttition的大小受限于单个
        挂载点的大小,log.dirs参数配置了partition的存储位置

        Partition Allocation
        当创建topic时,kafka决定如何在broker之间分配partition,假设有6台broker创建一个有10个分区的partition副本是3,此时就会有30个分区
        副本要分配到6台机器上,要达到以下目标
            平均副本到6台机器上也就是说一台机器5个副本
            确保每个分区的副本在不同的机器上,比如partition 0的leader在broker 2上,那它的followers在3,4上,不能在2上或者同时在3或4上
            如果有在不同机架,那么分区的副本也应该在不同的机架上,提高高可用性
        先决定leader partition的位置,在决定follower的位置,round-robin的方式,如果有rack则把机架考虑进去(原来是按0-5现在按0,3,1,4,2,5
        0-2在一个机架,3-4在一个机架)
        一旦确定了partition和replicas要存在哪个broker上,就开始确定用哪个目录来存储partition,做这件事对每一个partition是独立的,规则也很
        简单,查看每个目录中partition的个数,把新的partition放到数目最少的目录中(partition的分配不考虑磁盘的大小,所以要注意partition的大小
        broker的负载均衡)

        File Management
        持久化在kafka中是一个很重要的概念,kafka不会永久保留数据,而是保留一段时间的数据,或者保留一定大小的数据.因为在一个很大的文件中查找需要
        删除的数据是耗时的且容易出错的,kafka的做法是把partition切分成segments,默认情况下,每个segment要么是1GB的数据要么保留一周的数据,总
        的来说就小了,随着kafkabroker向partition写文件,如果segment的大小限制达到了,就会关闭这个segment然后重新打开一个新的segment,当前
        正在写的segment叫做active segment,active segment永远不会被删除,如果设置log retention的时间是一天,但是每个segment包含5天的
        数据,此时就会保留5天的数据,因为broker不会删除打开的segment,如果选择保留7天的数据,当每天都产生一个新的segment,所以每天都会删除一个
        最老的segment,而且分区总是有7个segment
        kafka为每个segment打开一个file handle(包括inactive的segment),所以要调整系统打开文件描述符的个数

        File Format
        每个segment保存在一个单独的数据文件中,在文件的内部保存消息和offsets
        kafka消息格式在producer和broker和consumer是一样的,这就为zero-copy带来了可能,也不需要压缩和解压缩,这些在prducer端已经做好了
        每个消息除了包括kv和offsets外还包含消息的大小,checksum code来侦测消息是否被修改,magic bytes指定消息格式的版本,compression
        code(Snappy GZip LZ4)和时间戳(0.10.0版本加入的)时间戳可以是producer发送的时间或者broker接收到的时间取决于如何配置
        如果生产者发送的是压缩的消息,那么在同一个batch中的消息都会被压缩到一起,作为wrapper message的值发送,所以broker收到的是一条消息,然后
        把这个消息发给consumer,consumer解压后能看到各个消息自己的内容(timestamp and offsets)
        这就意味着,如果在producer使用压缩(推荐使用),发送大批次的消息,意味着更高的压缩率,对网络传输和磁盘都有好处
        ./bin/kafka-run-class.sh kafka.tools.DumpLogSegments --files /tmp/kafka-logs/java_topic-0/
        00000000000000000000.log --print-data-log
        使用这个工具可以查看一个segment的内容

        Index
        kafka允许consumer从任意位置消费消息,这就需要快速的定位消息了,kafka为每个partition维护了一个index,index maps offsets to
        segment files and positions within the file
        indexes同样也分成segment,所有能够删除老的index当老的消息删除时,index文件时没有checksum的当index文件破坏了broker能够从数据文件
        重新构建index文件,所以删除index文件是安全的

        Compaction 消息的聚合,基于key只保留最新的消息 去重
        通常情况下kafka会保留一段时间的消息,还有种策略是按key删除消息,只保留相同key最新的消息(比如客户的收获地址,系统崩溃前的状态),这种策略
        是compact但是只对有kv的消息有效(如果没有key那么compaction会失败)
        每个日志都被分成两个片段
        Clean
            这些消息之前被处理过,每个key只有一个对应的value,这个值是上次处理保留下来的
        Dirty
            这些消息是上一次清理之后写入的
        如果在kafka启动时开启了清理功能(compaction)(log.cleaner.enabled)每个broker开启一个compaction manager thread和一些
        compaction thread(负责conpaction任务)这些线程会选择dirty ratio(污浊消息占总分区大小的比例)较高的分区进行清理
        为了compact一个partition,cleaner thread reads dirty section of the partition 然后创建一个map,map的key是消息key的hash值
        16 byte,value是8 byte的上个有相同key的消息的offset,这就意味着每个map entry只有24 bytes,假设一个segment是1GB,一个message有1
        KB,也就是说有100万的消息,但是这个map只用了24MB的内存来处理compaction,可能会更少,因为会有相同的key
        当配置kafka时,可以配置这些线程能够使用的内存总大小,假设分配了1GB的内存,有5个cleaner threads那么每个线程能用200MB的内存来维护这个
        map,当cleaner thread工作时,它会遍历section从最就的消息开始,一一比对生成新的segment代替旧的,然后在处理下一个segment,当合并完成
        后,offset是不连续的

        Deleted Events
        上面介绍了如何保留最新的消息,kafka还可以删除指定的key的消息,需要producer给那个key一个null的值,当cleaner处理这个消息时还是按之前的
        方式处理,但是最后会把这个消息标记成墓碑消息,会保留一段时间,在这段时间内consumer必须能够消费到这个消息,当发现这个消息时,consumer需要
        到数据库中把这个key的消息删除掉

        什么时候发生compact
        和delete策略相同,compact不会对当前active的segments做清理工作,只有inactive的segments才会做compact
        在0.10.0和之前的版本kafka在有50%的topic包含dirty数据时开始compact.目标是不要太频繁的做compact因为会影响性能,但也不要保留太多的
        dirty数据,浪费磁盘空间


     */
}
