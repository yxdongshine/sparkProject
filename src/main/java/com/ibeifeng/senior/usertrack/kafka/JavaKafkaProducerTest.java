package com.ibeifeng.senior.usertrack.kafka;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by ibf on 02/25.
 */
public class JavaKafkaProducerTest {
    public static void main(String[] args) {
        String topicName = "yxdkafka0";
        String brokerList = "hadoop1:9092,hadoop1:9093,hadoop1:9094";
        String partitionerClass = "com.ibeifeng.senior.usertrack.kafka.JavaKafkaPartitioner";
        int threadNums = 3;
        AtomicBoolean isRunning = new AtomicBoolean(true);
        JavaKafkaProducer producer = new JavaKafkaProducer(topicName, brokerList, partitionerClass);
        producer.run(threadNums, isRunning);


        // 停留60秒后，进行关闭操作
        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            // nothings
        }
        isRunning.set(false);

        // 关闭连接
        producer.closeProducer();
    }
}
