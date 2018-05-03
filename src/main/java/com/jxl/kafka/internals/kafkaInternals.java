package com.jxl.kafka.internals;

/**
 * Created by zhujiadong on 2018/5/2 10:15
 */

public class kafkaInternals {
    /*
        kafka用zookeeper来维护集群broker成员,每个broker都有一个唯一ID,每当一个broker进程开启后,都会以临时节点的方式注册自己的ID,
        kafka的组件订阅zookeeper中/brokers/ids的路径,所以当broker加入或者退出时都会得到通知
        当broker失去与zookeeper的连接时(broker stop ping or network partition or long GC pause),此时创建的临时节点会自动从
        zookeeper中移除,正在watching broker list的kafka组件会得到通知(broker挂了)
        关闭一个broker并不会删除它的id信息,依然会保留在其他数据结构中,比如每个topic的副本集会保留这个信息,如果一个新的broker以相同的id启动
        那么他会充当之前挂掉的broker的角色,分配属于它的replicas等等

        controller节点
        除了有普通broker的功能之外还能负责partition leader的选举,集群中第一个启动的节点会成为controller,创建临时节点 /controller,其他
        的节点启动时也会创建这个临时节点但是会收到一个 node already exists exception这就告诉其他broker集群中已经有controller这个角色了
        其他brokers在controller node上创建zookeeper watch对象来观察controller节点的变化,通过这种方式保证集群中同一时刻只有一个
        controller
        当controller节点关闭了或者失去与zookeeper的连接时,临时节点(controller创建的)会消失,其他brokers通过zookeeper watch知道了
        controller已经不在了,这些broker会尝试自己成为controller,第一个成功的broker会成为controller,其他broker收到node already
        exists exception并在新的controller上重新创建zookeeper watch对象,每当新的controller诞生,zookeeper会给它分配一个新的更大
        的controller epoch数字,其他broker知道当前的controller epoch的数字,如果收到来自小于这个数字的controller的消息,那么会忽略
        该消息
        当controller观察到broker离开了集群(通过观察zookeeper中相关的路径),controller知道在那台broker上的partition的leader需要一个
        新的leader,它会遍历所有需要新leader的分区,来决定谁来做老大,通常是replicas list中的第一个,给所有包含新leader或者相关followers
        的brokers发送requests,request包含新leader和followers的信息,新leader知道它需要接收consumer和producer的请求,followers知道
        从新的leader同步消息
        如果controller观察到新的broker加入到集群中(观察/brokers/ids),它会用新加入的broker id去检查是否包含现有分区的副本,如果有
        controller会把变更通知发送给新加入的broker和其他broker,然后新加入的broker上的replicas开始从leader上同步消息
        总的来说,kafka用zookeeper的ephemeralnode的特性来选举controller并通知controller是否有节点加入或移出集群,controller负责分区中
        leader的选举当有broker加入或移出集群时,使用epoch number避免发生split brain当两个broker都认为自己是当前的controller

        zookeeper负责broker中controller的选举,controller负责partition的replicas中leader的选举

        replication
        副本是kafka架构的核心,kakfa文档中第一句就把kafka描述成 a distributed, partitioned, replicated commit log services,
        replication是非常重要的它保证了当节点故障下的可用性和持久性
        kafka的消息是按topic组织的,topic又是分区的,每个分区都有自己的副本,每个副本都保存在broker上,通常每个broker保存几百甚至几千个
        属于不同topic和partition的replicas
        两种类型的replicas
        leader replica
            每个分区的副本中都有一个被设计为leader,所有的produce和consume请求都是通过leader完成的,这样也保证了一致性
        follower replica
            副本中其他的节点都是follower角色,followers不接受客户端的请求,它唯一的工作就是从leader同步消息,并且保证up-to-date,一旦
            leader挂了,follower顶上去
        leader还需要知道那个follower是同步到最新的消息,follwer同步消息不一定是最新的比如网络故障或者broker挂了,为了保证stay in sync
        with leader,副本会发送fetch requests,和consumer的请求相同去消费消息,为了响应这些请求,leader发送消息给replicas,这些fetch
        request包含了消息的offsets(replicas想要的消息),并且总是有序的
        一个replica会请求msg1,然后msg2,然后msg3,在没有收到前面消息的响应时,是不会请求msg4的,这样leader在replica请求msg4时,就知道它同步
        到消息3了,通过查看每个replica的最新offset,leader知道每个replica的同步进度,如果有一个副本在10秒内没有发起同步请求或者在10秒内请求的
        不是最新的消息,那么这个replica被认为是不同步的(out of sync),如果一个副本不能追上leader那么在leader发生故障,这个replica是不会成为
        新leader的,相反追上leader的replica叫做in sync replicas(ISR)这些replicas是有资格成为leader的
        replica.lag.time.max.ms控制一个replica被认为out of sync的总时间
        除了current leader外每个partition还有一个叫做preferred leader的角色,当新创建topic时,此时的preferred leader就是leader,因为
        要在broker之间负均衡,当preferred leader成为leader时,负载是均衡的,kafka默认配置auto.leader.rebalance.enable=true,当
        preferred leader不是当前的leader但是在ISR中时会触发leader election来让preferred leader变成current leader
        在ISR列表中的第一个总是preferred leader
     */
}
