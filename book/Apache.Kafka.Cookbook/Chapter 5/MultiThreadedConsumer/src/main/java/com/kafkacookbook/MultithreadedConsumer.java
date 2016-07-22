package com.kafkacookbook;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by saurabh on 28/7/15.
 */
public class MultithreadedConsumer {
    public static void main(String[] argv){
        (new MultithreadedConsumer()).run();
    }

    private void run() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("group.id", "mygroup");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put("mytesttopic", 4);


        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get("mytesttopic");

        ExecutorService executor = Executors.newFixedThreadPool(4);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerThread(stream, threadNumber));
            threadNumber++;
        }

        try { // without this wait the subsequent shutdown happens immediately before any messages are delivered
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        if (consumer != null) {
            consumer.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

}
