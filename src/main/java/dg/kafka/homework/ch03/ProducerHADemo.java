package dg.kafka.homework.ch03;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;

/**
 * 详细可以参考：https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 *
 * @author xenron
 */
public class ProducerHADemo {
    public static void main(String[] args) {

        // 设置配置属性
        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        // 创建producer
        Producer<String, String> producer = new Producer<String, String>(config);
        // 产生并发送消息
        long start = System.currentTimeMillis();
        long runtime = new Date().getTime();
        String key = "key";
        String msg = runtime + ", msg content";
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("test1", key, msg);
        producer.send(data);

        System.out.println("耗时:" + (System.currentTimeMillis() - start));
        // 关闭producer
        producer.close();
    }
}
