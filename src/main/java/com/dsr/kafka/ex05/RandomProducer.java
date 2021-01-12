package com.dsr.kafka.ex05;

import com.dsr.kafka.ex03.Supplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class RandomProducer {

    /**
     * Before running this program, run following commond on console, to create >1 partition for SupplierTopic
     * <p>
     * bin\windows\kafka-topics.bat --zookeeper localhost:2181 --create --topic SupplierTopic --partitions 3 --replication-factor 1
     **/


    public static void main(String[] args) {
        final String topic = "RandomTopic";
        //final String key = "supplier" ;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);
        String msg;
        ProducerRecord<String, String> record;

        int looper = 1;
        try {
            while (true) {
                for (int i = 0; i < 100; i++) {
                    record = new ProducerRecord(topic, 0, "key", String.valueOf(i * looper));
                    producer.send(record);
                    System.out.println("Sent on partition# " + 0 + ", value: " + i * looper);

                    record = new ProducerRecord(topic, 1, "key", String.valueOf(i * looper));
                    producer.send(record);
                    System.out.println("Sent on partition# " + 1 + ", value: " + i * looper);

                    try {
                        Thread.sleep(500);
                    } catch (Exception e) {
                        //nothing
                    }
                }
                looper++;
            }
        } catch (
                Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
