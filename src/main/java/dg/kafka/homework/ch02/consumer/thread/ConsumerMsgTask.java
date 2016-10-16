package dg.kafka.homework.ch02.consumer.thread;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class ConsumerMsgTask implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;

    /**
     * 该consumer的ID
     */
    private String consumerid;

    public ConsumerMsgTask(KafkaStream stream, int threadNumber) {
        m_threadNumber = threadNumber;
        m_stream = stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> streamIterator = m_stream.iterator();
        while (streamIterator.hasNext()) {
//            System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
            MessageAndMetadata<byte[], byte[]> message = streamIterator.next();
            String topic = message.topic();
            int partition = message.partition();
            long offset = message.offset();
            String key = new String(message.key());
            String msg = new String(message.message());
            // 在这里处理消息,这里仅简单的输出
            // 如果消息消费失败，可以将已上信息打印到日志中，活着发送到报警短信和邮件中，以便后续处理
            System.out.println("consumerid:" + consumerid + ", thread : " + Thread.currentThread().getName()
                    + ", topic : " + topic + ", partition : " + partition + ", offset : " + offset + " , key : "
                    + key + " , mess : " + msg);
        }

        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
