### what is prettyQ
prettyQ是一个消息队列，参考了kafka的设计， 100%由golang实现， 包含客户端组件和服务端组件。prettyQ的设计目标是：
* 可用性：多副本自动容灾切换，副本之间以etcd实现raft选举，副本之间采用最终一致性
* 可扩展性：服务端采用分布式分片设计， 支持动态横向扩缩
* 持久化：append-only的文件存储模式
* 高性能：消息不仅以append-only模式持久化写入文件，同时在内存中保持一个缓存队列供快速消费， 缓存中保持最新的消息，超过缓存长度的消息则通过读取磁盘
* 查询：支持基于messageNum的消息回溯查询，支持消息的重复消费
* 存储：不仅是一个消息队列，同时也是顺序日志的存储与查询系统
* 有序性：系统不提供消息的在多个分片之间的有序性保证，有序性由业务端自行负责
* 投递确认：每一条消息都有一条投递确认
* 消费确认：不支持。由于消息是持久化和多副本存储的，因此系统尽力保证经过投递确认的消息会被持久化不丢失，但这不是绝对的

### prettyQ的架构
![image]()
* prettyQ的pruducer/server, consumer/server之间的通信实现采用iip通信框架(https://github.com/truexf/iip)
* prettyQ的消息逻辑上按照topic分组，同一个topic的消息物理上分为多个连续的partition, 同一个partition包含的多个消息文件及索引，每个消息文件的容量相同，当一个消息文件写满，则写入下一个消息文件。
* 消息文件分为数据文件和索引文件：扩展名为.data的是消息文件，扩展名为.idx的是消息文件对应的稀疏索引文件，索引的key是messageNum, 每条message都有一个唯一num号
* 这些partition可分布式存储于不同的sharding server, sharding server以多replica set多副本模式实现高可用
* prettyQ的replica set内部、副本之间通过etcd(https://github.com/etcd-io/etcd)实现leader选举和状态一致性同步
* etcd中保存了prettyQ的元信息，包括：sharding, replica, topic，partiton
* producer和consumer连接到etcd，topic的元信息，根据这些元信息，决定连接具体的sharding server进行消息的发布和消费/查询

### prettyQ的消息格式
根据消息所处场景不同，分为三种格式：
#### 消息的存储格式 
消息文件中连续存储了n条规定格式的消息记录，一条消息记录的格式如下：
* 8Byte messageNum, BigEndian
* 4Byte messageSize, BigEndian
* nByte messageData
* 4Byte messageData crc32, BigEndian
消息文件名格式 topicName-startMessageNum.data|.idx
#### 消息的发布端格式
消息从producer发布到sharding server时的格式：
* 4Byte messageSize, BigEndian
* nByte messageData, 小于16MB 
* 4Byte messageData crc32, BigEndian
#### 消息的消费端格式
consumer从sharding server获取到的消息格式
* 8Byte messageNum, BigEndian
* 4Byte messageSize, BigEndian
* nByte messageData
* 4Byte messageData crc32, BigEndian
