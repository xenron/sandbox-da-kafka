package dg.kafka.homework.ch02.producer.thread;

import dg.kafka.homework.ch02.partitioner.RandomPartitioner;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 详细可以参考：https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 *
 * @author xenron
 */
public class ProducerDemo implements Runnable {
    private Producer<String, String> producer = null;

    private ProducerConfig config = null;

    private static AtomicLong next = new AtomicLong();


    public ProducerDemo() {
        Properties props = new Properties();
//        props.put("zookeeper.connect", "*****:2181,****:2181,****:2181");

//      props.put("zookeeper.connect", "localhost:2181");

        // 指定序列化处理类，默认为kafka.serializer.DefaultEncoder,即byte[]
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        // 同步还是异步，默认2表同步，1表异步。异步可以提高发送吞吐量，但是也可能导致丢失未发送过去的消息
        props.put("producer.type", "sync");

        // 是否压缩，默认0表示不压缩，1表示用gzip压缩，2表示用snappy压缩。压缩后消息中会有头来指明消息压缩类型，故在消费者端消息解压是透明的无需指定。
        props.put("compression.codec", "1");

        // 指定kafka节点列表，用于获取metadata(元数据)，不必全部指定
//        props.put("broker.list", "****:6667,***:6667,****:6667");
        props.put("metadata.broker.list", "127.0.0.1:9092");

        config = new ProducerConfig(props);
    }

    @Override
    public void run() {
        producer = new Producer<String, String>(config);
//      for(int i=0; i<10; i++) {
//          String sLine = "I'm number " + i;
//          KeyedMessage<String, String> msg = new KeyedMessage<String, String>("group1", sLine);
//          producer.send(msg);
//      }

//        for (int i = 1; i <= 6; i++) { //往6个分区发数据
//            List<KeyedMessage<String, String>> messageList = new ArrayList<KeyedMessage<String, String>>();
//            for (int j = 0; j < 6; j++) { //每个分区6条讯息
//                messageList.add(new KeyedMessage<String, String>
//                        //String topic, String partition, String message
//                        ("test1", "key[" + i + "]", "message[The " + i + " message]"));
//            }
//            producer.send(messageList);
//        }

        List<KeyedMessage<String, String>> messageList = new ArrayList<KeyedMessage<String, String>>();
        for (int i = 0; i < 4; i++) {
            long nextIndex = next.incrementAndGet();
            messageList.add(new KeyedMessage<String, String>
                    //String topic, String partition, String message
                    ("test1", "key[" + nextIndex + "]", "message[The " + nextIndex + " message]"));
        }
        producer.send(messageList);

    }

    public static void main(String[] args) {
        for (int i = 0; i < 4; i++) {
            Thread t = new Thread(new ProducerDemo());
            t.start();
        }

    }
}
