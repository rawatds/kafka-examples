package com.dsr.kafka.ex02;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sun.awt.geom.AreaOp;

import java.util.Date;
import java.util.Properties;

public class SensorProducer {

    public static void main(String[] args) throws InterruptedException {

        final String topic = "SensorTopic";


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", " org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", " org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.dsr.kafka.ex02.SensorPartitioner");
        props.put("speed.sensor.name", "TSS");

        Producer<String, String> producer = new KafkaProducer(props);

        for (int i = 0; i < 10; i++) {
            System.out.println("i = " + i);
            producer.send(new ProducerRecord(topic, "ABC" + i, "500" + i));
        }

        for (int j = 0; j < 10; j++) {
            System.out.println("j = " + j);
            producer.send(new ProducerRecord(topic, "TSS", "600" + j));
        }


        producer.close();

        System.out.println("Messages sent to topic:" + topic);

    }
}
