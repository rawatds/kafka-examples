package com.dsr.kafka.ex06.v1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AvroConsumer {

    public static void main(String[] args) {

        String topic = "AvroTopic";
        String groupName = "AvroGroup";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("group.id", groupName);
        props.put("key.deserializer", " org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader", "true");

        KafkaConsumer<String, ClickRecord> consumer = new KafkaConsumer(props);

        consumer.subscribe(Arrays.asList(topic));

        while (true) {

            ConsumerRecords<String, ClickRecord> records = consumer.poll(Duration.ofMillis(100));
            //System.out.println("Attempt : " + attempt++);
            for (ConsumerRecord<String, ClickRecord> record : records) {
                ClickRecord clickRec = record.value();
                System.out.println("The data is: "  + clickRec.getSessionId() + ", " + clickRec.getChannel() + ", " + clickRec.getIp());
            }

        }

    }
}
