package com.yiyang.kafka.seek;

import com.yiyang.kafka.capter1.ConsumerClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class SeekToEndDemo extends ConsumerClientConfig {

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        // timeout参数设置多少合适？太短会使分区分配失败，太长又有可能造成一些不必要的等待
        consumer.poll(Duration.ofMillis(2000));
        // 获取消费者所分配到的分区
        Set<TopicPartition> assignment = consumer.assignment();
        System.out.println(assignment);

        // 从指定分区末尾开始消费
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
            // 参数partition表示分区，offset表示指定从分区的哪个位置开始消费
            consumer.seek(tp, offsets.get(tp) + 1);
        }

        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String,String> record : records) {
                System.out.println(record.offset() + ":" + record.value());
            }
        }
    }
}
