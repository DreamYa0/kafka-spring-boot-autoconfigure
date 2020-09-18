package com.g7.framework.kafka.producer;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Executors;

/**
 * @author dreamyao
 * @title KafkaTemplate
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public interface KafkaOperations<K, V> {

    ListeningExecutorService LISTENING_EXECUTOR_SERVICE = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(3));


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
  