package com.kafkacookbook;

/**
 * Created by saurabh on 23/7/15.
 */
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConsumerGroupExample {
    private final ConsumerConnector consumer;
    private final String topic;

    public ConsumerGroupExample(String zookeeper, String groupId, String topic) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                consumerConfig);
        this.topic = topic;
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
        }

    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        int streamCount = 0;
        for (final KafkaStream stream : streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext())
                System.out.println("Thread " + streamCount + ": " + new String(it.next().message()));
            streamCount++;
        }
    }

    public static void main(String[] args) {
        String zooKeeper = "127.0.0.1:2181";
        String groupId = "consumer2";
        String topic = "mytesttopic";


        ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic);
        example.run();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        example.shutdown();
    }
}