package com.ibeifeng.senior.usertrack.kafka;

import org.apache.log4j.Logger;

/**
 * Created by ibf on 02/25.
 */
public class TestKafkaLog4j {

    public static final Logger logger = Logger.getLogger(TestKafkaLog4j.class);

    public static void main(String[] args) throws InterruptedException {
        for(int i=0;i<50;i++) {
            logger.debug("debug_" + i);
            logger.info("info_" + i);
            logger.warn("warn_" + i);
            logger.error("error_" + i);
            logger.fatal("fatal_" + i);

        }

        // 如果不sleep，会出现什么情况？怎么解决
        Thread.sleep(10000);
    }
}
