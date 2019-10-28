package com.g7.framework.kafka.producer;

import com.g7.framework.framwork.exception.BusinessException;
import com.g7.framework.framwork.exception.meta.CommonErrorCode;
import com.g7.framework.kafka.exception.KafkaProducerException;
import com.g7.framework.kafka.listener.LoggingProducerListener;
import com.g7.framework.kafka.listener.ProducerListener;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
import org.springframework.util.ObjectUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * <1> 若指定 Partition ID 则PR被发送至指定Partition
 * <2> 若未指定 Partition ID 但指定了Key, PR会按照hasy(key)发送至对应Partition
 * <3> 若既未指定 Partition ID 也没指定Key，PR会按照round-robin模式发送到每个Partition
 * <4> 若同时指定了 Partition ID 和 Key, PR只会发送到指定的Partition (Key不起作用，代码逻辑决定)
 * @author dreamyao
 * @title KafkaTemplate
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public class KafkaTransactionTemplate<K, V> implements KafkaOperations<K, V>, Lifecycle, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionTemplate.class);
    /**
     * 生产者
     */
    private Producer<K, V> producer;
    /**
     * 生成者监听器
     */
    private ProducerListener<K, V> producerListener = new LoggingProducerListener<>();
    /**
     * 是否自动冲刷（默认 否）
     */
    private AtomicBoolean autoFlush = new AtomicBoolean(false);
    /**
     * 是否已经启动
     */
    private volatile boolean running;

    public KafkaTransactionTemplate(Producer<K, V> producer) {
        this.producer = producer;
    }

    /**
     * 执行事物消息发送
     * @param supplier 生产者
     * @param <T>      返回结果
     * @return 结果
     */
    public <T> T transaction(Supplier<T> supplier) {

        // 初始化事物
        producer.initTransactions();

        // 执行返回结果
        T t;

        try {

            // 开启事物
            producer.beginTransaction();

            t = supplier.get();

            // 提交事务
            producer.commitTransaction();

        } catch (BusinessException be) {

            // 终止事务
            producer.abortTransaction();

            logger.info("Business exception caught in kafka transaction , rollback kafka message.", be);

            throw be;

        } catch (Throwable e) {

            // 终止事务
            producer.abortTransaction();

            logger.error("KafkaTransactionTemplate transaction error : {}", e.getMessage());

            BusinessException businessException = new BusinessException(CommonErrorCode.TRANSACTION_EXCEPTION);
            businessException.initCause(e);
            throw businessException;
        }

        return t;
    }

    /**
     * 发送消息（同步）
     * @param topic 主题
     * @param data  发送的数据
     * @return ListenableFuture
     */
    public ListenableFuture<RecordMetadata> send(String topic, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
        return doSend(producerRecord);
    }

    /**
     * 发送消息（同步）
     * @param topic 主题
     * @param key   将包含在记录中的密钥
     * @param data  发送的数据
     * @return ListenableFuture
     */
    public ListenableFuture<RecordMetadata> send(String topic, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
        return doSend(producerRecord);
    }

    /**
     * 发送消息（同步）
     * @param topic     主题
     * @param partition 分区编号
     * @param data      发送的数据
     * @return ListenableFuture
     */
    public ListenableFuture<RecordMetadata> send(String topic, Integer partition, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, null, data);
        return doSend(producerRecord);
    }

    /**
     * 发送消息（同步）
     * @param topic     主题
     * @param partition 分区编号
     * @param key       将包含在记录中的密钥
     * @param data      发送的数据
     * @return ListenableFuture
     */
    public ListenableFuture<RecordMetadata> send(String topic, Integer partition, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
        return doSend(producerRecord);
    }

    /**
     * 发送消息（同步）
     * @param topic     主题
     * @param partition 分区编号
     * @param timestamp 时间戳
     * @param key       将包含在记录中的密钥
     * @param value     发送的数据
     * @return ListenableFuture
     */
    public ListenableFuture<RecordMetadata> send(String topic, Integer partition, Long timestamp, K key, V value) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, timestamp, key, value);
        return doSend(producerRecord);
    }

    /**
     * 发送消息带回调（同步）
     * @param topic    主题
     * @param data     发送的数据
     * @param callback 回调实现
     */
    public void sendAndCallback(String topic, V data, FutureCallback<RecordMetadata> callback) {
        ListenableFuture<RecordMetadata> listenableFuture = send(topic, data);
        andCallback(callback, listenableFuture);
    }

    /**
     * 发送消息带回调（同步）
     * @param topic    主题
     * @param key      将包含在记录中的密钥
     * @param data     发送的数据
     * @param callback 回调实现
     */
    public void sendAndCallback(String topic, K key, V data, FutureCallback<RecordMetadata> callback) {
        ListenableFuture<RecordMetadata> listenableFuture = send(topic, key, data);
        andCallback(callback, listenableFuture);
    }

    /**
     * 发送消息带回调（同步）
     * @param topic     主题
     * @param partition 分区编号
     * @param data      发送的数据
     * @param callback  回调实现
     */
    public void sendAndCallback(String topic, Integer partition, V data, FutureCallback<RecordMetadata> callback) {
        ListenableFuture<RecordMetadata> listenableFuture = send(topic, partition, data);
        andCallback(callback, listenableFuture);
    }

    /**
     * 发送消息带回调（同步）
     * @param topic     主题
     * @param partition 分区编号
     * @param key       将包含在记录中的密钥
     * @param data      发送的数据
     * @param callback  回调实现
     */
    public void sendAndCallback(String topic, Integer partition, K key, V data, FutureCallback<RecordMetadata> callback) {
        ListenableFuture<RecordMetadata> listenableFuture = send(topic, partition, key, data);
        andCallback(callback, listenableFuture);
    }

    private void andCallback(FutureCallback<RecordMetadata> callback, ListenableFuture<RecordMetadata> listenableFuture) {
        ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
        Futures.addCallback(listenableFuture, callback, service);
    }

    /**
     * 发送消息(异步)且不关心发送结果
     * @param topic 主题
     * @param data  需要发送的数据
     */
    public void sendAsync(String topic, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
        doSendAsync(producerRecord);
    }

    /**
     * 发送消息(异步)且不关心发送结果
     * @param topic 主题
     * @param key   将包含在记录中的密钥
     * @param data  需要发送的数据
     */
    public void sendAsync(String topic, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
        doSendAsync(producerRecord);
    }

    /**
     * 发送消息(异步)通过回调来处理发送结果
     * @param topic           主题
     * @param data            需要发送的数据
     * @param messageCallBack 消息回调
     */
    public void sendAsync(String topic, V data, final MessageCallBack messageCallBack) {
        sendAsync(topic, null, null, data, messageCallBack);
    }

    /**
     * 发送消息(异步)通过回调来处理发送结果
     * @param topic           主题
     * @param key             将包含在记录中的密钥
     * @param data            需要发送的数据
     * @param messageCallBack 消息回调
     */
    public void sendAsync(String topic, K key, V data, final MessageCallBack messageCallBack) {
        sendAsync(topic, null, key, data, messageCallBack);
    }

    /**
     * 发送消息(异步)通过回调来处理发送结果
     * @param topic           主题
     * @param partition       分区编号
     * @param key             将包含在记录中的密钥
     * @param data            需要发送的数据
     * @param messageCallBack 消息回调
     */
    public void sendAsync(String topic, Integer partition, K key, V data, final MessageCallBack messageCallBack) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
        doSendAsync(producerRecord, messageCallBack);
    }

    /**
     * 发送消息(异步)通过回调来处理发送结果
     * @param topic           主题
     * @param partition       分区编号
     * @param timestamp       时间戳
     * @param key             将包含在记录中的密钥
     * @param value           需要发送的数据
     * @param messageCallBack 回调实现
     */
    public void sendAsync(String topic, Integer partition, Long timestamp, K key, V value, final MessageCallBack messageCallBack) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, timestamp, key, value);
        doSendAsync(producerRecord, messageCallBack);
    }

    /**
     * 获取原生Producer，自定义提交数据
     * @param callback 生产者回调
     * @return T
     */
    public <T> T execute(ProducerCallback<K, V, T> callback) {
        return callback.doInKafka(producer);
    }

    private void doSendAsync(final ProducerRecord<K, V> producerRecord) {

        doSendAsync(producerRecord, new MessageCallBack() {

            @Override
            public void onSuccess(RecordMetadata metadata) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Message send success for transaction. " + ObjectUtils.nullSafeToString(metadata));
                }
            }

            @Override
            public void onFail(Exception e) {
                logger.error("Message send failed for transaction. ", e);
            }

        });
    }

    private void doSendAsync(final ProducerRecord<K, V> producerRecord, final MessageCallBack messageCallBack) {

        if (logger.isTraceEnabled()) {
            logger.trace("Sending for transaction: " + producerRecord);
        }

        producer.send(producerRecord, (metadata, exception) -> {

            if (exception == null) {
                if (messageCallBack != null) {
                    messageCallBack.onSuccess(metadata);
                }
            } else {
                messageCallBack.onFail(exception);
            }
        });
    }

    private ListenableFuture<RecordMetadata> doSend(final ProducerRecord<K, V> producerRecord) {

        if (logger.isTraceEnabled()) {
            logger.trace("Sending for transaction: " + producerRecord);
        }

        final SettableFuture<RecordMetadata> settableFuture = SettableFuture.create();

        try {

            // 同步发送 超时时间10秒
            RecordMetadata recordMetadata = producer.send(producerRecord).get(10000, TimeUnit.MILLISECONDS);

            settableFuture.set(recordMetadata);
            if (producerListener != null && producerListener.isInterestedInSuccess()) {
                producerListener.onSuccess(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), producerRecord.value(), recordMetadata);
            }

        } catch (Exception e) {

            settableFuture.setException(new KafkaProducerException(producerRecord, "Failed to send for transaction. ", e));
            if (producerListener != null) {
                producerListener.onError(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), producerRecord.value(), e);
            }

            logger.error("Send message failed for transaction, topic is {} message is {}", producerRecord.topic(), producerRecord.value());
        }

        // 同步发送是否立即让kafka发送消息，即使 linger.ms 配置不等于 0 ms
        if (autoFlush.get()) {
            flush();
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Sent for transaction: " + producerRecord);
        }

        return settableFuture;
    }

    @Override
    public void flush() {

        producer.flush();
    }

    @Override
    public void destroy() {
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
            logger.error("Exception while stopping transaction producer. ", e);
        }
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    public KafkaTransactionTemplate<K, V> producerListener(ProducerListener<K, V> producerListener) {
        this.producerListener = producerListener;
        return this;
    }

    /**
     * 同步发送是否立即让kafka发送消息，即使 linger.ms 配置不等于 0 ms
     * @param autoFlush 是否
     * @return this
     */
    public KafkaTransactionTemplate<K, V> autoFlush(final boolean autoFlush) {
        this.autoFlush.set(autoFlush);
        return this;
    }
}
  