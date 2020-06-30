package com.yiyang.kafka.capter1;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka生产者‚‚
 */
public class ProducerFastStartTestUser {

    private static final String BROKER_LIST = "yiyang:9092";

    private static final String TOPIC_NAME = "yiyang";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        User user = User.builder().name("liuyiyang").address("杭州").build();
        // String userJson = JSONObject.toJSONString(user);
        ProducerRecord<String,User> record = new ProducerRecord<String,User>(TOPIC_NAME, user);
        try {
            // 异步发送
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
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
