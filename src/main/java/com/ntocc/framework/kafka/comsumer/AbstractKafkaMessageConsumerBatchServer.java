package com.ntocc.framework.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * kafka消息消费(批量消息消费)
 * @author dreamyao
 * @version 1.0.0
 * @date 2018/6/15 下午9:53
 */
public abstract class AbstractKafkaMessageConsumerBatchServer implements BatchMessageComsumer<String, String> {

    protected final Logger logger = LoggerFactory.getLogger(AbstractKafkaMessageConsumerBatchServer.class);

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> data) {

    }
}