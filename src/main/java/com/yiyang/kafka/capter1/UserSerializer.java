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
