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
