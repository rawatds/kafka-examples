package com.dsr.kafka.ex01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {

        final String topic = "MyFirstTopic";
        final String key = "developer.name";
        final String val = "Dharmender Singh Rawat at " + new Date();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", " org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", " org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String>producer = new KafkaProducer(props);

        ProducerRecord<String, String> record = new ProducerRecord(topic, key, val);

        producer.send(record);

        producer.close();

        System.out.println("Message sent to topic:" +  topic);

    }
}
