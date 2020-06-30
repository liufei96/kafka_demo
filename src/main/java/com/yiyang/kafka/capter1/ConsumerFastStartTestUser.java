package com.yiyang.kafka.capter1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * kafka的消费者
 */
public class ConsumerFastStartTestUser {

    private static final String BROKER_LIST = "yiyang:9092";

    private static final String TOPIC_NAME = "yiyang";

    private static final String GROUP_ID = "groupId.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeSerializer.class.getName());

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        KafkaConsumer<String,User> consumer = new KafkaConsumer<String, User>(properties);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String,User> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String,User> record : records) {
                System.out.println("消费的消息：" + record.value());
            }
        }
    }
}
