package com.g7.framework.kafka.listener;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public interface ProducerListener<K, V> {

    /**
     * Invoked after the successful send of a message (that is, after it has been acknowledged by the broker).
     * @param topic          the destination topic
     * @param partition      the destination partition (could be null)
     * @param key            the key of the outbound message
     * @param value          the payload of the outbound message
     * @param recordMetadata the result of the successful send operation
     */
    void onSuccess(String topic, Integer partition, K key, V value, RecordMetadata recordMetadata);

    /**
     * 消息发送失败后调用
     * @param topic     话题
     * @param partition 分区索引 (could be null)
     * @param key       the key of the outbound message
     * @param value     the payload of the outbound message
     * @param exception the exception thrown
     */
    void onError(String topic, Integer partition, K key, V value, Exception exception);

    /**
     * 如果这个Listener对成功感兴趣，就返回true
     * @return 对成功的发送感兴趣
     */
    boolean isInterestedInSuccess();
}
  