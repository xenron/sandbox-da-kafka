package com.kafkacookbook;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * Created by saurabh on 4/8/15.
 */
class ConsumerThread implements Runnable {
    private KafkaStream stream;
    private Integer threadNumber;

    public ConsumerThread(KafkaStream stream, int threadNumber) {
        this.stream = stream;
        this.threadNumber = threadNumber;
    }

    public void run() {

        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("Message from thread " + threadNumber + ": " + new String(it.next().message()));
        }
        System.out.println("Shutting down thread: " + threadNumber);
    }
}
