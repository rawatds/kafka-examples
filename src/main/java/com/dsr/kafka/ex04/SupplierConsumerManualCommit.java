package com.dsr.kafka.ex04;

import com.dsr.kafka.ex03.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SupplierConsumerManualCommit {

    public static void main(String[] args) {

        String topic = "SupplierTopic";
        String groupName = "SupplierTopicGroup";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("group.id", groupName);
        props.put("key.deserializer", " org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.dsr.kafka.ex03.SupplierDeserializer");
        props.put("enable.auto.commit", false);

        KafkaConsumer<String, Supplier> consumer = new KafkaConsumer(props);

        consumer.subscribe(Arrays.asList(topic));

        try {
            //int attempt =0;
            while (true) {

                ConsumerRecords<String, Supplier> records = consumer.poll(Duration.ofMillis(100));
                //System.out.println("Attempt : " + attempt++);
                for (ConsumerRecord<String, Supplier> record : records) {
                    Supplier supplier = record.value();
                    System.out.println("The data is: " + record.partition() + ":" + record.key() + ":" + supplier);
                }

                consumer.commitSync();
                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.commitSync();
        }
    }
}
