package com.dsr.ver2.ex1;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {

    private static final Logger logger =  LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Starting Producer ...");

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootStrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        for (int i = 0; i <AppConfig.numRecords; i++) {
            producer.send(new ProducerRecord<>(AppConfig.topic, i, "Message : " + i));
        }

        producer.close();

        logger.info("Finished.");
    }

}
