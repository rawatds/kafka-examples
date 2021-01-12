package com.dsr.ver2.multithread;

public interface AppConfig {

    String applicationId = "MultipThreadedProducer";
    String bootStrapServers = "localhost:9092, localhost:9093";
    String topic = "multi-topic";
    String[] files = {"data/a.csv", "data/b.csv", "data/c.csv"};


}
