package com.dsr.kafka.ex03;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SupplierProducerSync {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final String topic = "SupplierTopic";
        //final String key = "supplier" ;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.dsr.kafka.ex03.SupplierSerializer");
        props.put("num.partitions", 2);

        Producer<String, Supplier> producer = new KafkaProducer(props);

        for (int i=0; i < 1000; i++) {
            Supplier supplier = new Supplier(i, "Dharmender", new Date(), i * 1.0001);
            //ProducerRecord<String, Supplier> record = new ProducerRecord(topic, String.valueOf(i), supplier);
            ProducerRecord<String, Supplier> record = new ProducerRecord(topic, String.valueOf(i), supplier);
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Saved record " + i + " in partition# " + metadata.partition());

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();

        System.out.println("Message sent to topic:" + topic);
    }
}
