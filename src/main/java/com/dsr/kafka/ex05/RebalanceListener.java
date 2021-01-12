package com.dsr.kafka.ex05;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RebalanceListener implements ConsumerRebalanceListener {

    private KafkaConsumer consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();

    public RebalanceListener(KafkaConsumer con) {
        this.consumer = con;
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    public void addOffset(String topic, int partition, long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "commit"));
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Following partitions revoked ...");
        for (TopicPartition partition: partitions) {
            System.out.println(partition.partition() + ", ");
        }

        System.out.println("Following partitions committed ...");
        for(TopicPartition tp : currentOffsets.keySet()) {
            System.out.println(tp.partition());
        }

        //Commit and reset list
        consumer.commitSync(currentOffsets);
        currentOffsets.clear();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Following partitions assigned ...");
        for (TopicPartition partition: partitions) {
            System.out.println(partition.partition() + ", ");
        }
    }
}
