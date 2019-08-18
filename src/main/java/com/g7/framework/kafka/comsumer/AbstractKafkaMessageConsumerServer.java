package com.g7.framework.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka消费者,消费单条消息
 * @author dreamyao
 * @version 1.0.0
 * @date 2018/6/15 下午9:53
 */
public abstract class AbstractKafkaMessageConsumerServer<K, V> implements MessageComsumer<K, V> {

    protected final Logger logger = LoggerFactory.getLogger(AbstractKafkaMessageConsumerServer.class);

    @Override
    public void onMessage(ConsumerRecord<K, V> data) {

    }
}