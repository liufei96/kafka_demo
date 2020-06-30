package com.yiyang.kafka.commit;

import com.yiyang.kafka.capter1.ConsumerClientConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CommitSynclnRebalance  extends ConsumerClientConfig {

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

}
