package com.billionway.framework.kafka.producer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.billionway.framework.kafka.listener.LoggingProducerListener;
import com.billionway.framework.kafka.listener.ProducerListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
import org.springframework.util.ObjectUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author dreamyao
 * @title KafkaTemplate
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public class KafkaTemplate<K, V> implements KafkaOperations<K, V> , Lifecycle, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTemplate.class);
    private Producer<K, V> producer;
    private volatile String defaultTopic;
    private volatile ProducerListener<K, V> producerListener = new LoggingProducerListener<>();
    private final boolean autoFlush;
    private volatile boolean running;

    /**
     * 默认消息回掉
     */
    private final MessageCallBack defaultMessageCallBack = new MessageCallBack() {
        @Override
        public void onSuccess(RecordMetadata metadata) {
            if (logger.isTraceEnabled()) {
                logger.trace("message send success. " + ObjectUtils.nullSafeToString(metadata));
            }
        }

        @Override
        public void onFail(Exception e) {
            logger.error("message send fail. ", e);
        }

    };

    public KafkaTemplate(Producer<K, V> producer,boolean autoFlush) {
        this.autoFlush = autoFlush;
        this.producer = producer;
    }

    public ListenableFuture<Boolean> sendDefault(V data) {
        return send(this.defaultTopic, data);
    }

    public ListenableFuture<Boolean> sendDefault(K key, V data) {
        return send(this.defaultTopic, key, data);
    }

    public ListenableFuture<Boolean> sendDefault(int partition, K key, V data) {
        return send(this.defaultTopic, partition, key, data);
    }

    public void sendAndCallback(String topic, V data, FutureCallback<Boolean> callback) {
        ListenableFuture<Boolean> listenableFuture = send(topic, data);
        andCallback(callback, listenableFuture);
    }

    public void sendAndCallback(String topic, K key, V data, FutureCallback<Boolean> callback) {
        ListenableFuture<Boolean> listenableFuture = send(topic, key, data);
        andCallback(callback, listenableFuture);
    }

    public void sendAndCallback(String topic, Integer partition, V data, FutureCallback<Boolean> callback) {
        ListenableFuture<Boolean> listenableFuture = send(topic, partition, data);
        andCallback(callback, listenableFuture);
    }

    public void sendAndCallback(String topic, Integer partition, K key, V data, FutureCallback<Boolean> callback) {
        ListenableFuture<Boolean> listenableFuture = send(topic, partition, key, data);
        andCallback(callback, listenableFuture);
    }

    private void andCallback(FutureCallback<Boolean> callback, ListenableFuture<Boolean> listenableFuture) {
        ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
        Futures.addCallback(listenableFuture, callback, service);
    }

    public ListenableFuture<Boolean> send(String topic, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
        return doSend(producerRecord);
    }

    public ListenableFuture<Boolean> send(String topic, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
        return doSend(producerRecord);
    }

    public ListenableFuture<Boolean> send(String topic, Integer partition, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, null, data);
        return doSend(producerRecord);
    }

    public ListenableFuture<Boolean> send(String topic, Integer partition, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
        return doSend(producerRecord);
    }

    /**
     * 获取原生Producer，自定义提交数据
     * @param callback 生产者回调
     * @return
     */
    public <T> T execute(ProducerCallback<K, V, T> callback) {
        try {
            return callback.doInKafka(producer);
        } finally {
            producer.close();
        }
    }

    /**
     * 发送消息(异步)
     * @param topic 主题
     * @param data 需要发送的数据
     * @return 记录元数据
     */
    public Future<RecordMetadata> sendAsync(String topic, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
        return doSendAsync(producerRecord);
    }

    /**
     * 发送消息(异步)
     * @param topic 主题
     * @param key
     * @param data 需要发送的数据
     * @return 记录元数据
     */
    public Future<RecordMetadata> sendAsync(String topic, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
        return doSendAsync(producerRecord);
    }

    /**
     * 发送消息(异步)
     * @param topic 主题
     * @param data 需要发送的数据
     * @param messageCallBack 消息回调
     * @return 记录元数据
     */
    public Future<RecordMetadata> sendAsync(String topic, V data, final MessageCallBack messageCallBack) {
        return sendAsync(topic, null, null, data, messageCallBack);
    }

    /**
     * 发送消息(异步)
     * @param topic 主题
     * @param key
     * @param data 需要发送的数据
     * @param messageCallBack 消息回调
     * @return 记录元数据
     */
    public Future<RecordMetadata> sendAsync(String topic, K key, V data, final MessageCallBack messageCallBack) {
        return sendAsync(topic, null, key, data, messageCallBack);
    }

    /**
     * 发送消息(异步)
     * @param topic 主题
     * @param partition 分区
     * @param key
     * @param data 需要发送的数据
     * @param messageCallBack 消息回调
     * @return 记录元数据
     */
    public Future<RecordMetadata> sendAsync(String topic, Integer partition, K key, V data, final MessageCallBack messageCallBack) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
        return doSendAsync(producerRecord, messageCallBack);
    }

    private Future<RecordMetadata> doSendAsync(final ProducerRecord<K, V> producerRecord) {
        return doSendAsync(producerRecord, defaultMessageCallBack);
    }

    private Future<RecordMetadata> doSendAsync(final ProducerRecord<K, V> producerRecord, final MessageCallBack messageCallBack) {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending: " + producerRecord);
        }
        return producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                if (messageCallBack != null) {
                    messageCallBack.onSuccess(metadata);
                }
            } else {
                messageCallBack.onFail(exception);
            }
        });
    }

    private ListenableFuture<Boolean> doSend(final ProducerRecord<K, V> producerRecord) {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending: " + producerRecord);
        }
        final SettableFuture<Boolean> settableFuture = SettableFuture.create();
        producer.send(producerRecord, (metadata, exception) -> {
            try {
                if (exception == null) {
                    settableFuture.set(true);
                    if (producerListener != null && producerListener.isInterestedInSuccess()) {
                        producerListener.onSuccess(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), producerRecord.value(), metadata);
                    }
                } else {
                    settableFuture.setException(new KafkaProducerException(producerRecord, "Failed to send", exception));
                    if (producerListener != null) {
                        producerListener.onError(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), producerRecord.value(), exception);
                    }
                }
            } finally {
                producer.close();
            }
        });
        if (this.autoFlush) {
            flush();
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Sent: " + producerRecord);
        }
        return settableFuture;
    }

    @Override
    public void flush() {
        try {
            producer.flush();
        } finally {
            producer.close();
        }
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public ProducerListener<K, V> getProducerListener() {
        return producerListener;
    }

    public void setProducerListener(ProducerListener<K, V> producerListener) {
        this.producerListener = producerListener;
    }

    @Override
    public void destroy() throws Exception {
        Producer<K, V> copyProducer = producer;
        producer = null;
        if (copyProducer != null) {
            copyProducer.close();
        }
        running = false;
    }

    @Override
    public void start() {
        this.running = true;
    }

    @Override
    public void stop() {
        try {
            destroy();
        } catch (Exception e) {
            logger.error("Exception while stopping producer", e);
        }
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }
}
  