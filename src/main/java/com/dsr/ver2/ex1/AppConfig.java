package com.dsr.ver2.ex1;

public interface AppConfig {

    String applicationId = "HelloProducer";
    String bootStrapServers = "localhost:9092, localhost:9093";
    String topic = "hello-topic";
    int numRecords = 100;

}
