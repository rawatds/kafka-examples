package com.dsr.kafka.ex03;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class SupplierProducer {

    /**
     Before running this program, run following commond on console, to create >1 partition for SupplierTopic

     bin\windows\kafka-topics.bat --zookeeper localhost:2181 --create --topic SupplierTopic --partitions 3 --replication-factor 1

    **/


    public static void main(String[] args) {
        final String topic = "SupplierTopic";
        //final String key = "supplier" ;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.dsr.kafka.ex03.SupplierSerializer");

        Producer<String, Supplier> producer = new KafkaProducer(props);

        for (int i=0; i < 100; i++) {
            Supplier supplier = new Supplier(i, "Dharmender", new Date(), i * 1.0001);
            ProducerRecord<String, Supplier> record = new ProducerRecord(topic, String.valueOf(i), supplier);
            producer.send(record);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }

        producer.close();

        System.out.println("Message sent to topic:" + topic);
    }
}
