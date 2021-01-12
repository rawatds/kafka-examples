package com.dsr.kafka.ex01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SynchronousProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final String topic = "MyFirstTopic";
        final String key = "developer.name" + new Date();
        final String val = "Dharmender Singh Rawat at " + new Date();

        System.out.println(new Date());
        System.out.println(new Date().toString().length());


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", " org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", " org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);

        ProducerRecord<String, String> record = new ProducerRecord(topic,  key, val);

        try {
            RecordMetadata recMetaData = producer.send(record).get();

            System.out.println("Message sent to topic:" + topic);

            System.out.println(recMetaData.partition() + ", " + recMetaData.offset() + ", " + recMetaData.timestamp());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } finally {
            producer.close();
        }
    }
}
