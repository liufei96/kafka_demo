package com.yiyang.kafka.capter1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * kafka的消费者
 */
public class ConsumerFastStart {

    private static final String BROKER_LIST = "yiyang:9092";

    private static final String TOPIC_NAME = "yiyang";

    private static final String GROUP_ID = "groupId.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // properties.put("bootstrap.servers", BROKER_LIST);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        // properties.put("group.id", GROUP_ID);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        // consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        // 使用正则表达式指定订阅的topic
        consumer.subscribe(Pattern.compile("yiyang*"));
        // 指定订阅的分区
        consumer.assign(Arrays.asList(new TopicPartition("topic", 0)));
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String,String> record : records) {
                System.out.printf("消费的消息：" + record.value());
            }
        }
    }
}
