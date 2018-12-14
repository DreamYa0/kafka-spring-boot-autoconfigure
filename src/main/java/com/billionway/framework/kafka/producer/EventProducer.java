package com.billionway.framework.kafka.producer;

import com.billionway.framework.kafka.listener.ProducerListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public class EventProducer {

    private static final Logger logger = LoggerFactory.getLogger(EventProducer.class);
    private final Producer<String, Object> producer;

    public EventProducer(Producer<String, Object> producer) {
        this.producer = producer;
    }
    /**
     * 异步发送消息
     * @param topic    主题名称
     * @param content  消息内容
     * @param <E>      类型参数
     * @param listener 异步消息发送监听器
     */
    public <E> void asyncSend(String topic, E content, ProducerListener<String, Object> listener) {
        Objects.requireNonNull(topic, "topic should not be null");
        Objects.requireNonNull(content, "content msg should not be null");
        Objects.requireNonNull(producer, "kafkaProducer should not be null");

        try {

            long start = System.currentTimeMillis();
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, content.getClass().getName(), content);
            // 异步发送，可能会出现消息丢失，可以使用在允许消息丢失的场景
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {

                if (Objects.nonNull(listener) && listener.isInterestedInSuccess() && Objects.isNull(exception)) {

                    listener.onSuccess(topic, record.partition(), record.key(), record.value(), metadata);

                } else if (Objects.nonNull(listener) && Objects.nonNull(exception)) {

                    logger.error("------------- Async publish event failed. -------------", exception);
                    listener.onError(topic, record.partition(), record.key(), record.value(), exception);

                }
            });

            logger.info("-------------- Topic name: {} , Async publish event time: {} ms --------------", topic, System.currentTimeMillis() - start);
        } catch (Exception e) {
            throw new KafkaException("------------- Async publish event failed. -------------", e);
        }
    }

    /**
     * 同步发送消息
     * @param topic   主题名称
     * @param content 消息内容
     * @param <E>     类型参数
     */
    public <E> void syncSend(String topic, E content) {
        Objects.requireNonNull(topic, "topic should not be null");
        Objects.requireNonNull(content, "content msg should not be null");

        try {

            long start = System.currentTimeMillis();
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, content.getClass().getName(), content);

            // 同步发送，不会出现消息丢失
            producer.send(record).get(3000, TimeUnit.MILLISECONDS);
            logger.info("-------------- Topic name: {} , Sync send event time: {} ms --------------", topic, System.currentTimeMillis() - start);

        } catch (Exception e) {
            throw new KafkaException("------------- Sync send event failed. -------------", e);
        }
    }
}
