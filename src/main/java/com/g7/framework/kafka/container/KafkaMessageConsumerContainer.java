package com.g7.framework.kafka.container;

import com.g7.framework.kafka.comsumer.BatchMessageComsumer;
import com.g7.framework.kafka.comsumer.GenericMessageComsumer;
import com.g7.framework.kafka.comsumer.MessageComsumer;
import com.g7.framework.kafka.factory.KafkaConsumerFactory;
import com.g7.framework.kafka.properties.ContainerProperties;
import com.g7.framework.kafka.properties.KafkaProperties;
import com.g7.framework.kafka.util.ReadPropertiesUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 消费者容器
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午9:53
 * @since 1.0.0
 */
public class KafkaMessageConsumerContainer<K, V> extends AbstractMessageConsumerContainer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageConsumerContainer.class);
    /**
     * 消费者监听器
     */
    private ConsumerListener consumerListener;
    /**
     *
     */
    private Future<?> listenerConsumerFuture;

    @Autowired
    private KafkaProperties properties;

    public KafkaMessageConsumerContainer(ContainerProperties containerProperties) {
        super(containerProperties);
    }

    /**
     * 启动操作
     */
    @Override
    public void doStart() {

        if (isRunning()) {
            return;
        }

        ContainerProperties containerProperties = getContainerProperties();

        // 获取消费者组ID
        String groupId = containerProperties.getGroupId();

        // 获取消费者
        GenericMessageComsumer messageConsumer = containerProperties.getMessageConsumer();
        Assert.state(messageConsumer != null, "A message comsumer is required");

        if (containerProperties.getConsumerTaskExecutor() == null) {

            SimpleAsyncTaskExecutor consumerExecutor = new SimpleAsyncTaskExecutor(new ThreadFactoryBuilder().setNameFormat((getBeanName() == null ? "" : getBeanName()) + "-%d").build());
            consumerExecutor.setConcurrencyLimit(containerProperties.getQueueDepth());
            containerProperties.setConsumerTaskExecutor(consumerExecutor);
        }

        // 设置当前消费者组里面消费者的个数
        for (int i = 0; i < containerProperties.getQueueDepth(); i++) {

            listenerConsumerFuture = containerProperties.getConsumerTaskExecutor().submit(new ConsumerListener(messageConsumer, groupId));
        }

        setRunning(true);
    }

    /**
     * 关闭操作
     * @param callback callback
     */
    @Override
    protected void doStop(final Runnable callback) {

        if (!isRunning()) {
            return;
        }

        try {

            listenerConsumerFuture.get();
            if (logger.isDebugEnabled()) {
                logger.debug(KafkaMessageConsumerContainer.this + " stopped normally");
            }

        } catch (Exception e) {

            logger.error("Error while stopping the container", e);
        }

        if (callback != null) {
            callback.run();
        }

        setRunning(false);

        consumerListener.consumer.wakeup();
    }

    private KafkaConsumerFactory<K, V> createKafkaConsumerFactory() {

        Properties consumerDefaultProperties = ReadPropertiesUtils.readConsumerDefaultProperties();

        consumerDefaultProperties.setProperty("bootstrap.servers", properties.getBootstrap().getServers());

        getConsumerDeserializer(consumerDefaultProperties);

        return new KafkaConsumerFactory<>(consumerDefaultProperties);
    }

    private void getConsumerDeserializer(Properties consumerDefaultProperties) {

        String keyDeserializer = properties.getConsumer().getKeyDeserializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(keyDeserializer))) {
            consumerDefaultProperties.setProperty("key.deserializer", keyDeserializer);
        }

        String valueDeserializer = properties.getConsumer().getValueDeserializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(valueDeserializer))) {
            consumerDefaultProperties.setProperty("value.deserializer", valueDeserializer);
        }
    }

    /**
     * Consumer监听线程
     * @author dreamyao
     */
    private final class ConsumerListener implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(ConsumerListener.class);
        private final ContainerProperties containerProperties = getContainerProperties();
        private final Consumer<K, V> consumer;
        private final MessageComsumer<K, V> messageComsumer;
        private final BatchMessageComsumer<K, V> batchMessageComsumer;
        private final KafkaConsumerFactory<K, V> consumerFactory = createKafkaConsumerFactory();

        /**
         * 自动提交
         */
        private final boolean autoCommit = consumerFactory.isAutoCommit();

        @SuppressWarnings("unchecked")
        protected ConsumerListener(GenericMessageComsumer genericMessageComsumer, String groupId) {

            final Consumer<K, V> consumer;

            if (StringUtils.isEmpty(groupId)) {

                consumer = consumerFactory.createConsumer();

            } else {

                consumer = consumerFactory.createConsumer4Group(groupId);
            }

            if (containerProperties.getTopicPattern() != null) {

                consumer.subscribe(containerProperties.getTopicPattern(), containerProperties.getConsumerRebalanceListener());

            } else {

                consumer.subscribe(Arrays.asList(containerProperties.getTopics()), containerProperties.getConsumerRebalanceListener());
            }

            this.consumer = consumer;

            if (genericMessageComsumer instanceof MessageComsumer) {

                messageComsumer = (MessageComsumer<K, V>) genericMessageComsumer;
                batchMessageComsumer = null;

            } else if (genericMessageComsumer instanceof BatchMessageComsumer) {

                messageComsumer = null;
                batchMessageComsumer = (BatchMessageComsumer<K, V>) genericMessageComsumer;

            } else {

                messageComsumer = null;
                batchMessageComsumer = null;

                logger.error("GenericMessageComsumer must implements BatchMessageComsumer or MessageComsumer");
            }
        }

        @Override
        public void run() {

            while (isRunning()) {

                try {

                    ConsumerRecords<K, V> records = consumer.poll(containerProperties.getPollTimeout());

                    if (records != null && logger.isDebugEnabled()) {
                        logger.debug("Received: " + records.count() + " records");
                    }

                    if (records != null && records.count() > 0) {
                        invokeListener(records);
                    }

                    // 判断是否自动提交
                    if (autoCommit) {
                        continue;
                    }

                    // 提交partitions
                    if (Objects.nonNull(records)) {

                        for (TopicPartition partition : records.partitions()) {

                            List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);

                            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

                            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                        }
                    }

                } catch (Exception e) {

                    logger.error("Comsumer message failed.", e);
                }
            }
        }

        /**
         * 消费业务分发
         * @param records kafka 消息记录
         */
        private void invokeListener(ConsumerRecords<K, V> records) {

            if (batchMessageComsumer != null) {

                invokeBatchRecordListener(records);

            } else if (messageComsumer != null) {

                invokeRecordListener(records);
            }
        }

        /**
         * 批量消费消息
         * @param records kafka 消息记录
         */
        private void invokeBatchRecordListener(ConsumerRecords<K, V> records) {

            List<ConsumerRecord<K, V>> recordList = new ArrayList<>();
            Iterator<ConsumerRecord<K, V>> iterator = records.iterator();

            while (iterator.hasNext()) {

                final ConsumerRecord<K, V> record = iterator.next();
                recordList.add(record);
            }

            if (recordList.isEmpty()) {
                return;
            }

            try {
                batchMessageComsumer.onMessage(recordList);
            } catch (Exception e) {
                logger.error("Consumer batch message failed , consumer name is {}", batchMessageComsumer.getClass().getName(), e);
            }
        }

        /**
         * 单个消费消息
         * @param records kafka 消息记录
         */
        private void invokeRecordListener(ConsumerRecords<K, V> records) {

            Iterator<ConsumerRecord<K, V>> iterator = records.iterator();

            while (iterator.hasNext()) {

                final ConsumerRecord<K, V> record = iterator.next();
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace("Processing " + record);
                }

                try {
                    messageComsumer.onMessage(record);
                } catch (Exception e) {
                    logger.error("Consumer message failed , consumer name is {}", messageComsumer.getClass().getName(), e);
                }
            }
        }
    }

    public ConsumerListener getConsumerListener() {
        return consumerListener;
    }

    public void setConsumerListener(ConsumerListener consumerListener) {
        this.consumerListener = consumerListener;
    }
}
  