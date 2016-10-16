package dg.kafka.homework.ch02.partitioner;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Round robin partitioner using a simple thread safe AotmicInteger
 */
public class RoundRobinPartitioner2 implements Partitioner {
    private static final Logger log = Logger.getLogger(RoundRobinPartitioner2.class);

    final AtomicInteger counter = new AtomicInteger(0);

    public RoundRobinPartitioner2(VerifiableProperties props) {
        log.trace("Instatiated the Round Robin Partitioner class");
    }

    /**
     * Take key as value and return the partition number
     */
    public int partition(Object key, int partitions) {

        int partitionId = counter.incrementAndGet() % partitions;
        if (counter.get() > 65536) {
            counter.set(0);
        }
        return partitionId;
    }
}
