package com.yiyang.kafka.capter1;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

public class UserDeSerializer implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String topic, byte[] data) {
        User user = new User();
        if (data == null) {
            return user;
        }
        try {
            String encoding = Base64.getEncoder().encodeToString(data);
            byte[] decode = Base64.getDecoder().decode(encoding);
            String str = new String(decode);
            String[] s = str.split(" ");
            user.setName("zs");
            user.setAddress("lisi");
            return user;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return user;
    }

    @Override
    public User deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }

    public static void main(String[] args) {
        User user = User.builder().name("liufei").address("杭州").build();
        UserSerializer userSerializer = new UserSerializer();
        byte[] yiyangs = userSerializer.serialize("yiyang", user);
        System.out.println(Arrays.toString(yiyangs));

        UserDeSerializer userDeSerializer = new UserDeSerializer();
        User yiyang = userDeSerializer.deserialize("yiyang", yiyangs);
        System.out.println(yiyang);
    }
}
