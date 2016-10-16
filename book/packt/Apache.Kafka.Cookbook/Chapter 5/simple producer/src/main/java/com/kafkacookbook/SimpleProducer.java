package com.kafkacookbook;

import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by saurabh on 22/7/15.
 */
public class SimpleProducer {
    public static void main(String [] args){
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "127.0.0.1:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
        for(int iCount = 0; iCount < 100; iCount++){
            String message = "My Test Message No "+iCount;
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("mytesttopic", message);
            producer.send(record);
        }
        producer.close();
    }
}
