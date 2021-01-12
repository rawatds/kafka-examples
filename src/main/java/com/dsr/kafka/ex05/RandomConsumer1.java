package com.dsr.kafka.ex05;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class RandomConsumer1 {

    public static void main(String[] args) {

        String topic = "RandomTopic";
        String groupName = "RandomTopicGroup";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", groupName);
        props.put("enable.auto.commit", "false");


        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        // Assign a RebalanceListener to consumer
        RebalanceListener rebalanceListener = new RebalanceListener(consumer);

        consumer.subscribe(Arrays.asList(topic), rebalanceListener);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : records) {
                    System.out.println("Processed by Consumer1 : Partition# " + record.partition() + ", Data: " + record.value());
                    rebalanceListener.addOffset(record.topic(), record.partition(), record.offset());
                }

                // Disabled for testing purpose only
                //consumer.commitSync(rebalanceListener.getCurrentOffsets());
            }
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
