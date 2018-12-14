package io.seg.framework.message.listener;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 消息监听适配器
 * @param <K>
 * @param <V>
 * @author dreamyao
 */
public abstract class ProducerListenerAdapter<K, V> implements ProducerListener<K, V> {

    @Override
    public void onSuccess(String topic, Integer partition, K key, V value, RecordMetadata recordMetadata) {
    }

    @Override
    public void onError(String topic, Integer partition, K key, V value, Exception exception) {
    }

    @Override
    public boolean isInterestedInSuccess() {
        return false;
    }

}