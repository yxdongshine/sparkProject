package com.ibeifeng.senior.usertrack.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Java实现的Kafka生产者
 * Created by ibf on 02/25.
 */
public class JavaKafkaProducer {
    public static final char[] chars = "qazwsxedcrfvtgbyhnujmikolp0123456789".toCharArray();
    public static final int charsLength = chars.length;
    public static final Random random = new Random(System.currentTimeMillis());
    private Producer<String, String> producer = null;
    private String topicName = null;
    private String brokerList = null;
    private boolean isSync = true; // 默认为同步
    private String partitionerClass = null; // 数据分区器class类

    /**
     * 构造函数
     *
     * @param topicName
     * @param brokerList
     */
    public JavaKafkaProducer(String topicName, String brokerList) {
        this(topicName, brokerList, true, null);
    }

    /**
     * 构造函数
     *
     * @param topicName
     * @param brokerList
     * @param partitionerClass
     */
    public JavaKafkaProducer(String topicName, String brokerList, String partitionerClass) {
        this(topicName, brokerList, true, partitionerClass);
    }

    /**
     * 构造函数
     *
     * @param topicName
     * @param brokerList
     * @param isSync
     * @param partitionerClass
     */
    public JavaKafkaProducer(String topicName, String brokerList, boolean isSync, String partitionerClass) {
        // 赋值
        this.topicName = topicName;
        this.brokerList = brokerList;
        this.isSync = isSync;
        this.partitionerClass = partitionerClass;

        // 1. 给定配置信息：参考http://kafka.apache.org/082/documentation.html#producerconfigs
        Properties props = new Properties();
        // kafka集群的连接信息
        props.put("metadata.broker.list", this.brokerList);
        // kafka发送数据方式
        if (this.isSync) {
            // 同步发送数据
            props.put("producer.type", "sync");
        } else {
            // 异步发送数据
            props.put("producer.type", "async");
            /**
             * 0: 不等待broker的返回
             * 1: 表示至少等待1个broker返回结果
             * -1：表示等待所有broker返回数据接收成功的结果
             */
            props.put("request.required.acks", "0");
        }
        // key/value数据序列化的类
        /**
         * 默认是：DefaultEncoder, 指发送的数据类型是byte类型
         * 如果发送数据是string类型，必须更改StringEncoder
         */
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        // 给定分区器的class参数
        if (this.partitionerClass != null && !this.partitionerClass.trim().isEmpty()) {
            // 默认是：DefaultPartiioner，基于key的hashCode进行hash后进行分区
            props.put("partitioner.class", this.partitionerClass.trim());
        }

        // 2. 构建Kafka的Producer Configuration上下文
        ProducerConfig config = new ProducerConfig(props);

        // 3. 构建Kafka的生产者：Producerr
        this.producer = new Producer<String, String>(config);
    }

    /**
     * 关闭producer连接
     */
    public void closeProducer() {
        producer.close();
    }

    /**
     * 提供给外部应用调用的直接运行发送数据代码的方法
     *
     * @param threadNumbers
     * @param isRunning
     */
    public void run(int threadNumbers, final AtomicBoolean isRunning) {
        for (int i = 0; i < threadNumbers; i++) {
            new Thread(new Runnable() {
                public void run() {
                    int count = 0;
                    while (isRunning.get()) {
                        // 只有在运行状态的情况下，才发送数据
                        KeyedMessage<String, String> message = generateMessage();
                        // 发送数据
                        producer.send(message);
                        count++;
                        // 打印一下
                        if (count % 100 == 0) {
                            System.out.println("Count = " + count + "; message:" + message);
                        }

                        // 假设需要休息一下
                        try {
                            Thread.sleep(random.nextInt(100) + 10);
                        } catch (InterruptedException e) {
                            // nothings
                        }
                    }
                    System.out.println("Thread:" + Thread.currentThread().getName() + " send message count is:" + count);
                }
            }).start();
        }
    }

    /**
     * 产生一个随机的Kafka的KeyedMessage对象
     *
     * @return
     */
    public KeyedMessage<String, String> generateMessage() {
        String key = generateString(3) + "_" + random.nextInt(10);
        StringBuilder sb = new StringBuilder();
        int numWords = random.nextInt(5) + 1; // [1,5]单词
        for (int i = 0; i < numWords; i++) {
            String word = generateString(random.nextInt(5) + 1); // 单词中字符最少1个最多5个
            sb.append(word).append(" ");
        }
        String message = sb.toString().trim();
        return new KeyedMessage(this.topicName, key, message);
    }

    /**
     * 随机生产一个给定长度的字符串
     *
     * @param numItems
     * @return
     */
    public static String generateString(int numItems) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numItems; i++) {
            sb.append(chars[random.nextInt(charsLength)]);
        }
        return sb.toString();
    }


}
