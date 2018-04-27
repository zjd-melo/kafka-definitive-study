package com.jxl.kafka.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by zhujiadong on 2018/4/24 15:16
 */

/**
 * 这种方式做序列化有很多弊端
 */
public class ProducerTest {
    private static Properties properties = new Properties();
    private static KafkaProducer<String, Customer> producer = null;
    static {
        ClassLoader classLoader = ProducerTest.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("kafkaproducer.properties");
        try{
            properties.load(inputStream);
            producer = new KafkaProducer<String, Customer>(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>("java_topic","name",new Customer(1,"melo"));
        long offset = producer.send(producerRecord).get().offset();
        System.out.println(offset);
    }
}
