package com.dsr.kafka.ex02;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class SensorPartitioner implements Partitioner {

    private String speedSensorName;
    private static int count = 0;

    public void configure(Map<String, ?> configs) {
        System.out.println("SensorPartitioner.configure()");
        speedSensorName = configs.get("speed.sensor.name").toString();
    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("Count: " + SensorPartitioner.count++ + ", key :" + key);

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int sp = (int) Math.abs(numPartitions * 0.3);
        int partition = 0;

        if (keyBytes == null || (!(key instanceof  String))){
            throw new InvalidRecordException("The keys should be String only");
        }

        if (((String)key).equals(speedSensorName)) {
            partition = Utils.toPositive(Utils.murmur2(valueBytes)) % sp;
        } else {
            partition = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - sp) + sp;
        }

        System.out.println("Partition #" + partition + " was calculated for key: " + key);

        return partition;
    }

    public void close() {
        System.out.println("SensorPartitioner.close()");
    }

}
