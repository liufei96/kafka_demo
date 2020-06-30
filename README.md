 





## 1.2 安装与配置

### 1.2.1 java环境的安装

解压tar包

```shell
tar -zvf jdk-8u144-linux-i586.tar.gz
```

```shell
# 修改环境变量 
vim /etc/profile

export JAVA_HOME=/usr/local/java/jdk1.8.0_144
export JRE_HOME=${JAVA_HOME}/jre 
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib:$CLASSPATH 
export JAVA_PATH=${JAVA_HOME}/bin:${JRE_HOME}/bin 
export PATH=$PATH:${JAVA_PATH}

# 重新加载配置文件
source /etc/profile

# 验证结果
java -version
```

问题：

如果输入java -version出现的是这个

```shell
-bash: /usr/local/java/jdk1.8.0_144/bin/java: /lib/ld-linux.so.2: bad ELF interpreter: 没有那个文件或目录
```

则使用下面命令即可

```shell
yum install glibc.i686
```

### 1.2.2 ZooKeeper的安装

Zookeeper是安装Kafka集群的必要组建，Kafka通过Zookeeper来实施对原数据信息的管理，包括集群、主题、分区等内容。

同样在官网下载安装包到指定目录解压缩，步骤如下：

- Zookeeper官网：https://zookeeper.apache.org/

- 修改Kafka的配置文件，首先进入安装路径conf目录，并将zoo_sample.cfg文件修改为zoo.cfg，并对核心参数进行配置。

  文件内容如下：

  ```shell
  # The number of milliseconds of each tick
  # ZK服务器的心跳时间
  tickTime=2000
  # The number of ticks that the initial 
  # synchronization phase can take
  # 投票选取新Leader的初始化时间
  initLimit=10
  # The number of ticks that can pass between 
  # sending a request and getting an acknowledgement
  syncLimit=5
  # the directory where the snapshot is stored.
  # do not use /tmp for storage, /tmp here is just 
  # example sakes.
  # 数据目录
  dataDir=/opt/apache-zookeeper-3.6.1-bin/data
  # 日志目录
  dataLogDir=/opt/apache-zookeeper-3.6.1-bin/log
  # the port at which the clients will connect
  clientPort=2181
  # the maximum number of client connections.
  # increase this if you need to handle more clients
  #maxClientCnxns=60
  #
  # Be sure to read the maintenance section of the 
  # administrator guide before turning on autopurge.
  #
  # http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
  
  ```

- 启动Zookeeper命令：bin/zkServer.sh start

```shell
[root@localhost bin]# zkServer.sh start
-bash: zkServer.sh: command not found
[root@localhost bin]# ./zkServer.sh start
ZooKeeper JMX enabled by default
Using config: /opt/apache-zookeeper-3.6.1-bin/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
```

### 1.2.3 Kafka的安装和配置

官网内容：http://kafka.apache.org/

- 下载解压成功后，开始启动

  启动命令：bin/kafka-server.start.sh config/server.properties

  **server.properties** 需要关注以下几个配置

> broker.id=0  表示broker的编号，如果集群中有多个broker，则每个broker的编号需要不同
>
> listeners=PLAINTEXT://:9092      broker对外提供的服务入口地址
>
> log.dirs=/opt/kafka_2.13-2.5.0/logs    设置存放消息日志文件的地址
>
> zookeeper.connect=localhost:2181     kafka所需zookeeper集群地址。

- 验证结果

```shell
[root@localhost bin]# jps -l
54723 sun.tools.jps.Jps
53387 kafka.Kafka
103581 org.apache.zookeeper.server.quorum.QuorumPeerMain
```

### 1.2.4 kafka测试消息的生产与消费

- 首先创建一个主题

命令如下：

```shell
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic yiyang --partitions 2 --replication-factor 1
```

> --zookeeper：制定了kafka所连接的zookeeper服务的地址
>
> --topic：指定了所要创建主题的名称
>
> --partitions：指定了分区个数
>
> --replication-factor：指定了副本因子
>
> --create：创建主题的动作命令

```shell
[root@localhost kafka_2.13-2.5.0]# bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic yiyang --partitions 2 --replication-factor 1
Created topic yiyang.
```

- 展示所有的主题

命令如下：bin/kafka-topics.sh --zookeeper localhost:2181 --list

```shell
[root@localhost kafka_2.13-2.5.0]# bin/kafka-topics.sh --zookeeper localhost:2181 --list
yiyang
```

- 查看主题详情

命令如下：bin/kafka-topics.sh --zookeeper localhost:2181 --describe  --topic yiyang

```shell
[root@localhost kafka_2.13-2.5.0]# bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic yiyang
Topic: yiyang   PartitionCount: 2       ReplicationFactor: 1    Configs: 
        Topic: yiyang   Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: yiyang   Partition: 1    Leader: 0       Replicas: 0     Isr: 0

```

- 启动消费端接收消息

命令：bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic yiyang

> --bootstrap-server 指定了连接kafka集群的地址
>
> --topic  指定了消费端订阅的主题



地址在开一个终端

- 生产端发送消息

命令：bin/kafka-console-producer.sh  --broker-list localhost:9092 --topic yiyang

> --broker-list  指定连接kafka集群的地址
>
> --topic  指定了发送消息时的主题

```shell
[root@localhost bin]# ./kafka-console-producer.sh --broker-list localhost:9092 --topic yiyang
>hello kafka
>
>nihao liufei
>nihao yiyang
>
```

此时在这个里面就可以输入消息

看下消费端的：

```shell
[root@localhost kafka_2.13-2.5.0]# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic yiyang
hello kafka

nihao liufei
nihao yiyang


```

## 1.3. Java第一个程序

通过Java程序来进行kafka首发消息



1.3.1 准备

kafka自身提供的提供的java客户端来演示消息的收发，与kafka的java客户端相关的Maven依赖。

```xml
<properties>
		<java.version>1.8</java.version>
		<kafka.version>2.3.1</kafka.version>
		<scala.version>2.12</scala.version>
		<spark.version>2.4.4</spark.version>
</properties>

<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-clients</artifactId>
			<version>${kafka.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka_${scala.version}</artifactId>
			<version>${kafka.version}</version>
			<exclusions>
				<exclusion>
					<groupId>org.apache.zookeeper</groupId>
					<artifactId>zookeeper</artifactId>
				</exclusion>
				<exclusion>
					<groupId>org.slf4j</groupId>
					<artifactId>slf4j-log4j12</artifactId>
				</exclusion>
				<exclusion>
					<groupId>log4j</groupId>
					<artifactId>log4j</artifactId>
				</exclusion>
			</exclusions>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-streaming_${scala.version}</artifactId>
			<version>${spark.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-streaming-kafka-0-10_${scala.version}</artifactId>
			<version>${spark.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-sql_${scala.version}</artifactId>
			<version>${spark.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-sql-kafka-0-10_${scala.version}</artifactId>
			<version>${spark.version}</version>
		</dependency>
```



## 1.4 服务daunt的常用参数配置

### 1.4.1 zookeeper.connect

指明Zookeeper主机地址，如果zookeeper是集群则以逗号一个开，如：

172.16.103.128,172.16.103.129,172.16.103.130

### 1.4.2 listeners

监听列表，broker对外提供服务时绑定的IP和端口，多个以逗号隔开，如果监听器名称不时一个安全的协议，listener,security.protocol.map也必须设置。主机名称设置0.0.0.0绑定所有的接口，主机名称为空则绑定默认的接口。如：PLAINTEXT://myhost:9092,SSL://:9091 CLIENT:0.0.0.0:9092,REPLICATION://locahost:9093

### 1.4.3 broker.id

broker的唯一标识符，如果不配置则自动生成。建议配置且一定要邦正集群中必须唯一，默认-1

### 1.4.4 log.dirs

日志数据存放的目录，如果没有配置则使用log.dir，建议配置此项配置。

### 1.4.5 message.max.bytes

服务器接收单个数据的最大大小，默认1000012约等于976.6KB。



# 第2章 生产者详解

## 2.1 消息发送

### 2.1.1 Kafka Java客户端数据生产流程解析

![image-20200625223050019](/Users/liufei/Library/Application Support/typora-user-images/image-20200625223050019.png)

### 2.1.2 必要参数配置

### 2.1.3 发送类型

发送即忘记

Producer.send(record)



**同步发送**

```java
// 通过send()发送完消息后返回一个Future对象，然后调用Future对象的get()方法等待kafka响应
// 如果kafka正常响应，返回一个RecordMetadata对象，该对象存储消息的偏移量
// 如果kafka发送错误，无法正常响应，就会抛出异常，我们便可以进行异常处理
 producer.send(record).get();
```

异步发送

```java
 producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (metadata != null) {
                        System.out.println("TOPIC:" + metadata.topic());
                        System.out.println("partition:" + metadata.partition());
                        System.out.println("offset:" + metadata.offset());
                    }
                }
            });
```



### 2.1.4 序列化器

消息要到网络上进行传输，必须进行序列化，而序列化器的作用就是如此。

Kafka提供了默认的字符串序列化器(org.apache.kafka.common.serialization.StringSerializer)，还有整形(IntegerSerializer)和字节数组(BytesSerializer)序列化器，这些序列化器都实现了接口（org.apache.kafka.common.serialization.Serializer），基本上能够满足大部分场景的需求。

```java
package com.yiyang.kafka.capter1;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class UserSerializer implements Serializer<User> {

    private String encoding = "UTF-8";

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, User data) {
        if (data == null) {
            return null;
        }
        byte [] name;
        byte [] address;
        try {
            if (data.getName() != null) {
                name = data.getName().getBytes(encoding);
            } else {
                name = new byte[0];
            }
            if (data.getAddress() != null) {
                address = data.getAddress().getBytes(encoding);
            } else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, User data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
```

还需要一个反序列化

### 2.1.5 分区器

本身kafka有自己的分区策略，如果未指定，就会使用默认的分区策略。

kafka根据传递消息的key来进行区分，即hash(key)%numpartitions。如果Key相同的话，那么就会分配到同一分区

源码分析：org.apache.kafka.clients.producer.internals.DefaultPartitioner

```java
 /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null) {
            int nextValue = nextValue(topic);
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return Utils.toPositive(nextValue) % numPartitions;
            }
        } else {
            // hash the keyBytes to choose a partition
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }
```

自定义分区器，代码库

```java
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyDefaultPartition  implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
       // TODO 这里面写逻辑
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
```

```java
// 使用自定义的分区
properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyDefaultPartition.class.getName());
```

### 2.1.6 拦截器

Producer拦截器(interceptor)是相当于新的功能，它和consumer端的interceptor是在kafka0.10版本引入的，主要用于实现clients端的定制化控制逻辑。



生产者拦截器可以用来消息发送钱做一些准备工作。

使用场景：

1、按照某个规则过滤掉不符合要求的消息

2、修改消息的内容

3、统计类需求



见代码库：自定义拦截器

```java
package com.yiyang.kafka.capter1;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class MyProducerInterceptor implements ProducerInterceptor {

    private volatile long sendSuccess = 0;
    private volatile long sendFailed = 0;

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        String newValue = "prefix-" + record.value();
        return new ProducerRecord(record.topic(), record.partition(),record.timestamp(),record.key(),newValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess++;
        } else {
            sendFailed++;
        }
    }

    @Override
    public void close() {
        double successPercentage = sendSuccess / (sendFailed + sendSuccess);
        System.out.println("[INFO]发送成功率=" + String.format("%f", successPercentage * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
```

然后在使用的地方加上：

```java
properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
```

## 2.2 发送原理剖析

![image-20200627164215936](/Users/liufei/Library/Application Support/typora-user-images/image-20200627164215936.png)

消息发送的过程中，设计到两个线程协同工作，主线程首先将业务数据封装成ProducerRecord对象，之后调用send()方法将消息放入RecordAccumulator（消息收集器，也可以理解为主线程与Sender线程直接的缓存区）中暂存，Sender线程负责将消息信息构成请求，并最终执行网络的I/O的线程，它从RecordAccumulator中取出消息并批量发送出去，需要注意的是，kafkaProducer是线程安全的，多个线程间可以共享使用同一个kafkaProducer对象

## 2.3 其他生产者参数

之前提及的默认三个客户端参数，大部分参数都是合理的默认值，一般情况下不需要修改它们

参考官网：http://kafka.apache.org/documentation/#producerconfigs

### 2.3.1 acks

这个参数用来指定分区中必须有多少个副本收到这条消息，之后生产者才会认为这条消息是写入成功的。acks是生产者客户端中非常重要的一个参数，它设置及到消息的可靠性和吞吐量之间的权衡。

- ack=0，生产者在成功写入消息之前不会等待任何来自服务器的响应，如果出现问题生产者是感知不到的，消息就丢失。不过因为生产者不需要等待服务器的响应，所以它可以以网络能够支持的最大速度发送消息，从而达到很高的吞吐量。
- acks=1，默认是1，只要集群的首领节点收到消息，生产者就会收到一个来自服务器的成功响应。如果消息无法达到首领节点（比如首领节点崩溃，新的首领还没有被选举出来），生产者会收到一个错误响应，为了避免数据丢失，生产者会重发消息。但是，这样还有可能导致数据丢失，如果收到写成功通知，此时首领节点还没来得及同步数据到fp;;pwer节点，首领节点崩溃，就会导致数据丢失。
- ack=-1，只有当所有参与复制的节点收到消息时，生产者会收到一个来自服务器的成功响应，这种模式是最安全的，它可以保证不止一个服务器收到消息。

注意：acks参数配置的是一个字符串类型，而不是整数类型，如果配置为整数类型就会跑出以下异常：

![image-20200627170233465](/Users/liufei/Library/Application Support/typora-user-images/image-20200627170233465.png)

### 2.3.2 retries

生产者从服务器收到的错误有可能是临时性的错误（比如分区找不到首领）。这种情况下，如果达到了retries设置的次数，生产者会放弃重试并返回错误。默认情况下，生产者会在每次重试之间等待100ms，通过retry.backoff.ms参数来修改这个时间间隔。

### 2.3.3 batch.size

当有多个消息要被发送到同一分区时，生产者和UI把他们放在用一个批次里。该参数指定了一个批次可以使用的内存大小，按照字节数计算，而不是消息个数，当批次被填满，批次里的所有消息会被发送出去。不过生产者并不一定都会等到批次被填满才发送，半满的批次，甚至只包含一个消息的批次也可能被发送。所以就算把batch.size设置的很大，也不会造成延迟，只会占用更多的内存而已，如果设置的太小，生产者会因为频繁发送消息而增加一些额外的开销。

### 2.3.4 max.request.size

该参数用来控制身缠这发送的请求大小，他可以指定能发送的单个消息的最大值，也可以指单个请求里所有消息的总大小。broker对可接收的消息最大值也是有自己的限制（message.max.size），所以两边的配置最好的匹配，避免生产者发送的消息被broker拒绝

## 总结：

本章主要讲了生产者客户端的用法以及整体流程架构，主要内容包含配置采纳数的详解、消息的发送方式、序列化器、分区器、拦截器等，在实际使用中，kafka以及提供了良好的Java客户端支持，提高开发效率



# 第3章 消费者详解

> tips 学完这一章你可以
>
> 深入学习Kafka数据消费大致流程
>
> 如何创建并使用Kafka消费者
>
> Kafka消费者常用配置

## 3.1 概念入门

3.1.1 消费者和消费者组

kafka消费者是消费者组的一部分，当多个消费者形成一个消费者组来消费主体时，每个消费者会收到不同分区的消息。假设有一个T1主题，该主题有4个分区；同时我们有一个消费组G1，这个消费组只有一个消费者C1。那么消费者C1将会收到这4个分区的消息，如下所示：

![image-20200627174623250](/Users/liufei/Library/Application Support/typora-user-images/image-20200627174623250.png)

Kafka一个很重要的 特性就是，只需写入一次消息，可以支持任意多的应用读取这个消息。换句话说，每个应用都可以读到全量的消息。为了使得每个应用都能读到全量消息，应用需要有不同的消费组。对于上面的例子，假如我们新增了一个新的消费组G2，而这个消费组有两个消费者，那么会是这样的。

![image-20200627174954500](/Users/liufei/Library/Application Support/typora-user-images/image-20200627174954500.png)

## 3.2 消息接收

见代码库：

```java
 Properties properties = new Properties();
    // 与KafkaProducer中设置保持一致
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeSerializer.class.getName());
		// 必填参数，该参数和kafkaProducer中的相同，制定连接的Kafka集群所需的Broker地址清单，可以设置一个或多个
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
   // 消费者隶属于的消费组，默认为空，如果设置为空，则会跑出异常，这个参数要设置成具有一定业务含义的名称
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
   // 指定KafkaConsumer对应的客户端Id，默认为空，如果不设置kafkaConsumer会自动生成一个非空的字符串
        properties.put("client.id", "consumer.client.id.demo");
        KafkaConsumer<String,User> consumer = new KafkaConsumer<String, User>(properties);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String,User> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String,User> record : records) {
                System.out.println("消费的消息：" + record.value());
            }
        }
```

### 3.2.2 订阅主题和分区

创建完消费者后我们便可以订阅主题了，只需要通过调用subscrib()方法即可，这个方法接收一个主题列表

```java
KafkaConsumer<String,User> consumer = new KafkaConsumer<String, User>(properties);
consumer.subscribe(Collections.singletonList(TOPIC_NAME));
```

另外，我们也可以使用正则表达式来匹配多个主题，而且订阅之后又有匹配新的主题，那么这个消费组会立即对其

进行消费。正则表达式在连接Kafka与其他系统时非常有用。比如订阅所有的测试主题：

```java
consumer.subscribe(Pattern.compile("yiyang*"));
```

指定订阅的分区

```java
// 指定订阅的分区
consumer.assign(Arrays.asList(new TopicPartition("topic226", 0)));
```

### 3.2.2 反序列化

```java
// 与KafkaProducer中设置保持一致
properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
```

### 3.2.3 位移提交

对于Kafka中的分区而言，它的每一条消息都有位移的offset，用来表示消息在分区的位置。

当我们调用poll()时，该方法会返回我们没有消费的消息。当消息从broker返回消费时，broker并不跟踪这些消息是否被消费者接收到；Kafka让消费者自身来管理消费的位移，并向消费者提供更新位移的接口，这种更新位移的方式称之为提交（commit）。

#### **重复消费**

![image-20200627182015273](/Users/liufei/Library/Application Support/typora-user-images/image-20200627182015273.png)

#### **消息丢失**

![image-20200627182054474](/Users/liufei/Library/Application Support/typora-user-images/image-20200627182054474.png)

#### **自动提交**

这种方式让消费者来管理位移，应用本身不需要显示操作。当我们将enable.auto.commit设置为true，那么消费者会在poll方法调用后每隔5秒（由auto.commit.interval.ms指定）提交一次位移。和很多其他操作一样，自动提交也是由poll()方法来驱动；在调用poll()时，消费者判断是否到达提交时间，如果是则提交上一次poll返回的最大位移。

需要注意到，这种方式可能会导致消息重复消费。假如，某个消费者poll消息后，应用正在处理消息，在3秒后Kafka进行了重平衡，那么由于没有更新位移导致重平衡后，这部分消息重复消费

#### **同步提交**

```java
package com.yiyang.kafka.capter1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class CheckOffsetAndCommit {

    private static final String BROKER_LIST = "yiyang:9092";

    private static final String TOPIC_NAME = "yiyang";

    private static final String GROUP_ID = "groupId.demo";

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 关闭自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = getProperties();
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition tp = new TopicPartition(TOPIC_NAME, 0);
        consumer.assign(Arrays.asList(tp));
        long lastConsumerOffset = -1;
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(1000);
            if (records.isEmpty()) {
                break;
            }
            List<ConsumerRecord<String,String>> partitionsRecords = records.records(tp);
            lastConsumerOffset = partitionsRecords.get(partitionsRecords.size() - 1).offset();
            // 同步提交消费位移
            consumer.commitAsync();
            System.out.println("消费的内容：" + partitionsRecords.get(partitionsRecords.size() - 1).value());
        }
        System.out.println("consumer offset is " + lastConsumerOffset);
        OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
        System.out.println("commited offset is " + offsetAndMetadata.offset());
        long position = consumer.position(tp);
        // 下次消费的位置
        System.out.println("the offset of the next record is " + position);
    }
}
```

#### 异步提交

手动提交有一个缺点，那就是当发起提交调用时应用汇阻塞。当然我们可以减少手动提交的频率，但这个会增加消息重复的概率（和自动提交一样）。另外一个解决办法是，使用一部提交的API

见代码：

但是异步提交也有个缺点，那就是如果服务器返回提交失败，异步提交不会进行重试。相比较起来，同步提交会进行重试直到成功或者最后跑出异常给应用。异步提交没有实现重试是因为，如果同时存在多个异步提交，进行重试可能导致位移覆盖。举个例子，假如我们发起了一个异步提交commitA，此时的提交位移为2000，随后又发起了一个异步提交commitB位移为3000；commitA提交失败但commitB提交成功，此时commitA进行重试并成功的话，会将实际上将已经提交的位移从3000回滚到2000，导致消费重复消费。

异步回调：

```java
Properties properties = getProperties();
KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
consumer.subscribe(Arrays.asList(TOPIC_NAME));
try {
    while (running.get()) {
        ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String,String> record : records) {

        }

        // 异步调用
        consumer.commitAsync(new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception == null) {
                    System.out.println(offsets);
                } else {
                    log.error("fail to commit offsets {}", offsets, exception);
                }
            }
        });
    }
} finally {
    consumer.close();
}
```

### 3.2.4 指定位移消费

到目前为止，我们知道消息的拉取是根据poll()方法中的逻辑来处理的，但是这个方法对于普通开发人员来说就是个黑盒处理，无法精确掌握其消费的起始位置。

seek()方法正好提供了这个功能，让我们得以追踪以前的消费或者回溯消费。

```java
public static void main(String[] args) {
    Properties properties = initConfig();
    KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Arrays.asList(TOPIC_NAME));
    // timeout参数设置多少合适？太短会使分区分配失败，太长又有可能造成一些不必要的等待
    consumer.poll(Duration.ofMillis(2000));
    // 获取消费者所分配到的分区
    Set<TopicPartition> assignment = consumer.assignment();
    System.out.println(assignment);

    for (TopicPartition tp : assignment) {
        // 参数partition表示分区，offset表示指定从分区的哪个位置开始消费
        consumer.seek(tp, 10);
    }

    while (true) {
        ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String,String> record : records) {
            System.out.println(record.offset() + ":" + record.value());
        }
    }
}
```

- 如果没有获取到分区，则继续获取分区，知道获取到

```java
public static void main(String[] args) {
    Properties properties = initConfig();
    KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Arrays.asList(TOPIC_NAME));
    // timeout参数设置多少合适？太短会使分区分配失败，太长又有可能造成一些不必要的等待
    consumer.poll(Duration.ofMillis(2000));
    // 获取消费者所分配到的分区
    Set<TopicPartition> assignment = consumer.assignment();
    System.out.println(assignment);
    // 如果没有获取到分区，则继续获取分区
    while (assignment.size() == 0) {
        consumer.poll(Duration.ofMillis(100));
        assignment = consumer.assignment();
    }

    for (TopicPartition tp : assignment) {
        // 参数partition表示分区，offset表示指定从分区的哪个位置开始消费
        consumer.seek(tp, 10);
    }

    while (true) {
        ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String,String> record : records) {
            System.out.println(record.offset() + ":" + record.value());
        }
    }
}
```

- 指定从分区末尾开始消费

```java
// 从指定分区末尾开始消费
Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
for (TopicPartition tp : assignment) {
    // 参数partition表示分区，offset表示指定从分区的哪个位置开始消费
    consumer.seek(tp, offsets.get(tp));
}
```

- 演示位移越界操作，修改代码如下：

```java
// 从指定分区末尾开始消费
Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
for (TopicPartition tp : assignment) {
    // 参数partition表示分区，offset表示指定从分区的哪个位置开始消费
    consumer.seek(tp, offsets.get(tp) + 1);
}
```

会通过auto offset reset参数的默认值将位置重置。效果如下

### 3.2.5 再均衡监听器

再均衡是指分区的所属从一个消费者转移到另一个消费者的行为，它为消费者组具备了高可用性和伸缩性提供了保障，使得我们既方便又安全地删除消费组内的消费者或者往消费组内添加消费者。不过再均衡发生期间，消费者是无法拉取消息的。

代码如下：`com.yiyang.kafka.commit.CommitSynclnRebalance`

```java
private static  final AtomicBoolean isRunning = new AtomicBoolean(true);

public static void main(String[] args) {
    Properties properties = initConfig();
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    consumer.subscribe(Arrays.asList(TOPIC_NAME), new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // 尽量避免重复消费
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        }
    });
    try {
        while (isRunning.get()) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record: records) {
                System.out.println(record.offset() + ":" + record.value());
                // 异步提交消费位移，再发生在均衡动作之前可以通过再均衡监听器的oonPartitionsRevoked回调执行commitSync()方法
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            }
            consumer.commitAsync(currentOffsets, null);
        }
    } finally {
        consumer.close();
    }
}
```

### 3.2.6 消费者监听器

之前章节讲了生产者拦截器，对应的消费者也有相应的拦截器概念，消费者拦截器主要实在消费到消息或者在提交消费位移时进行的一些定制化的操作。

**使用场景**

对消费者消息设置了一个有效期的属性，如果某条消息在既定的时间窗口内无法到达，那就视为无效，不需要再被处理。

代码如下：`com.yiyang.kafka.consumer.interceptor.ConsumerInterceptorTTL`

```java
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String,String> {

    private static final long EXPIRE_INTERVAL = 10 * 1000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        System.out.println("before:" + records);
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String,String>>> newRecords = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> tpRecord = records.records(tp);
            List<ConsumerRecord<String,String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String,String> record : tpRecord) {
                if (now - record.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
```

消费者加上拦截器配置

```java
properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorTTL.class.getName());
```

生产端发布消息

```java
// 往前移了10s
ProducerRecord<String,String> record2 = new ProducerRecord<String,String>(TOPIC_NAME,
        0,
        System.currentTimeMillis() - 10 * 500,
        "kafka-demo",
        "hello，我是翊扬 -> 超时了");
```