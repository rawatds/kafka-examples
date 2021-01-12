package com.dsr.kafka.ex06.v1;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AvroProducer {


    // Download and install this file manually on your local .m2 repo
    // http://packages.confluent.io/maven/io/confluent/
    //
    // mvn install:install-file -DgroupId=io.confluent -DartifactId=kafka-avro-serializer -Dversion=6.0.1 -Dpackaging=jar -Dfile=C:\Users\dharmender.rawat\Downloads\kafka-avro-serializer-6.0.1.jar
    // mvn install:install-file -DgroupId=io.confluent -DartifactId=common-config -Dversion=6.0.1 -Dpackaging=jar -Dfile=C:\Users\dharmender.rawat\Downloads\common-config-6.0.1.jar
    // mvn install:install-file -DgroupId=io.confluent -DartifactId=kafka-schema-serializer -Dversion=6.0.1 -Dpackaging=jar -Dfile=C:\Users\dharmender.rawat\Downloads\kafka-schema-serializer-6.0.1.jar
    // mvn install:install-file -DgroupId=io.confluent -DartifactId=kafka-schema-registry -Dversion=6.0.1 -Dpackaging=jar -Dfile=C:\Users\dharmender.rawat\Downloads\kafka-schema-registry-6.0.1.jar
    // mvn install:install-file -DgroupId=io.confluent -DartifactId=kafka-schema-registry-client -Dversion=6.0.1 -Dpackaging=jar -Dfile=C:\Users\dharmender.rawat\Downloads\kafka-schema-registry-client-6.0.1.jar
    // Ref: https://stackoverflow.com/questions/43488853/confluent-maven-repository-not-working

    public static void main(String[] args) {

        final String topic = "AvroTopic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        Producer<String, ClickRecord>producer = new KafkaProducer(props);

        ClickRecord clickRec = new ClickRecord();
        clickRec.setSessionId("10001");
        clickRec.setChannel("PC/Browser");
        clickRec.setIp("192.168.0.1");

        ProducerRecord<String, ClickRecord> record = new ProducerRecord(topic, clickRec.getSessionId(), clickRec);

        producer.send(record);

        producer.close();

        System.out.println("Message sent to topic:" +  topic);

    }
}
