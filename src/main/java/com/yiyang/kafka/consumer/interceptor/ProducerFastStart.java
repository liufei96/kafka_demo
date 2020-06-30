package com.yiyang.kafka.consumer.interceptor;

import com.yiyang.kafka.capter1.MyProducerInterceptor;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka生产者‚‚
 */
public class ProducerFastStart {

    private static final String BROKER_LIST = "yiyang:9092";

    private static final String TOPIC_NAME = "yiyang";

    public static void main(String[] args) {
        // kafka集群地址
        Properties properties = new Properties();
        // 设置key序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        // 设置值序列化
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
        // 设置集群地址
        // 设置ack
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

        // 使用自定义的分区
        // properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyDefaultPartition.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(TOPIC_NAME, "kafka-demo", "大家好，我是翊扬");
        // 往前移了10s
        ProducerRecord<String,String> record2 = new ProducerRecord<String,String>(TOPIC_NAME,
                0,
                System.currentTimeMillis() - 10 * 500,
                "kafka-demo",
                "hello，我是翊扬 -> 超时了");
        try {
            // 同步发送
            producer.send(record);
            producer.send(record2);

        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
