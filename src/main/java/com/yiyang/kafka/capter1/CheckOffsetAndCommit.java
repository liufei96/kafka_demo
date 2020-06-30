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
