package dg.kafka.homework.ch02;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round robin partitioner using a simple thread safe AotmicInteger
 */
public class RandomPartitioner implements Partitioner {
//    public int partition(String key,int numPartitions)
//    {
//        //System.out.println("Fuck!!!!");
//        System.out.print("partitions number is "+numPartitions+"   ");
//        if (key == null) {
//            Random random = new Random();
//            System.out.println("key is null ");
//            return random.nextInt(numPartitions);
//        }
//        else {
//            int result = Math.abs(key.hashCode())%numPartitions; //很奇怪，
//            //hashCode 会生成负数，奇葩，所以加绝对值
//            System.out.println("key is "+ key+ " partitions is "+ result);
//            return result;
//        }
//    }

    @Override
    public int partition(Object o, int numPartitions) {
        String key = o.toString();
        //System.out.println("Fuck!!!!");
        System.out.print("partitions number is "+numPartitions+"   ");
        if (key == null) {
            Random random = new Random();
            System.out.println("key is null ");
            return random.nextInt(numPartitions);
        }
        else {
            int result = Math.abs(key.hashCode())%numPartitions; //很奇怪，
            //hashCode 会生成负数，奇葩，所以加绝对值
            System.out.println("key is "+ key+ " partitions is "+ result);
            return result;
        }
    }
}
