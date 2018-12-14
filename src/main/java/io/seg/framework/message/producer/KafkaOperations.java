package io.seg.framework.message.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author dreamyao
 * @title KafkaTemplate
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public interface KafkaOperations<K, V> {


    /**
     * Flush the producer.
     */
    void flush();


    /**
     * A callback for executing arbitrary operations on the {@link Producer}.
     * @param <K>
     * @param <V>
     * @param <T>
     * @author dreamyao
     */
    interface ProducerCallback<K, V, T> {

        T doInKafka(Producer<K, V> producer);
    }

    interface MessageCallBack {

        void onSuccess(RecordMetadata metadata);

        void onFail(Exception e);
    }
}
  