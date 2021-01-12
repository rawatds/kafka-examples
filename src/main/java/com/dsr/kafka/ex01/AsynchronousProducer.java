package com.dsr.kafka.ex01;

import org.apache.kafka.clients.producer.*;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AsynchronousProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final String topic = "MyFirstTopic";
        final String key = "developer.name" + new Date();
        final String val = "Dharmender Singh Rawat at " + new Date();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", " org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", " org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);

        ProducerRecord<String, String> record = new ProducerRecord(topic, key, val);

        producer.send(record, new MyProducerCallback());

        System.out.println("Message sent to topic:" + topic);
        producer.close();

    }
}

class MyProducerCallback implements Callback {

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

        if (e != null) {
            System.out.println("Error in async call: " + e.getMessage());
        } else {
            System.out.println("Async message sent succeeded");
            System.out.println(recordMetadata.partition() + ", " + recordMetadata.offset() + ", " + recordMetadata.timestamp());
        }
    }
}
