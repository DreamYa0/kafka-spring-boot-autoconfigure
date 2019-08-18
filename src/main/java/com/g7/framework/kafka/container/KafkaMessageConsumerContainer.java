package com.g7.framework.kafka.container;

import com.g7.framework.kafka.comsumer.BatchMessageComsumer;
import com.g7.framework.kafka.comsumer.GenericMessageComsumer;
import com.g7.framework.kafka.comsumer.MessageComsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * 消费者容器
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午9:53
 * @since 1.0.0
 */
public class KafkaMessageConsumerContainer<K, V> extends AbstractMessageConsumerContainer {

    private static final Log logger = LogFactory.getLog(KafkaMessageConsumerContainer.class);
    /**
     * 配置文件
     */
    private final KafkaConsumerFactory<K, V> consumerFactory;
    private final ContainerProperties containerProperties;
    private ConsumerListener consumerListener;
    private Future<?> listenerConsumerFuture;
    /**
     * 是否正在执行
     */
    private volatile boolean running = false;

    public KafkaMessageConsumerContainer(KafkaConsumerFactory<K, V> consumerFactory, ContainerProperties containerProperties) {
        super(containerProperties);
        this.containerProperties = containerProperties;
        this.consumerFactory = consumerFactory;
        Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
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

        // 如果有消费者组ID则设置
        String groupId = containerProperties.getGroupId();

        // 判断是否自动提交
        GenericMessageComsumer messageConsumer = containerProperties.getMessageConsumer();
        Assert.state(messageConsumer != null, "A message comsumer is required");

        if (containerProperties.getConsumerTaskExecutor() == null) {

            SimpleAsyncTaskExecutor consumerExecutor = new SimpleAsyncTaskExecutor((getBeanName() == null ? "" : getBeanName()) + "-C-");
            containerProperties.setConsumerTaskExecutor(consumerExecutor);
        }

        int many = containerProperties.getManyComsumer();
        for (int i = 0; i < many; i++) {

            listenerConsumerFuture = containerProperties.getConsumerTaskExecutor().submit(new ConsumerListener(messageConsumer, groupId));
        }

        setRunning(true);
    }

    /**
     * 关闭操作
     * @param callback
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
            logger.error("Error while stopping the container: ", e);
        }
        if (callback != null) {
            callback.run();
        }
        setRunning(false);
        consumerListener.consumer.wakeup();
    }

    public ConsumerListener getConsumerListener() {
        return consumerListener;
    }

    public void setConsumerListener(ConsumerListener consumerListener) {
        this.consumerListener = consumerListener;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void setRunning(boolean running) {
        this.running = running;
    }

    public KafkaConsumerFactory<K, V> getConsumerFactory() {
        return consumerFactory;
    }

    @Override
    public ContainerProperties getContainerProperties() {
        return containerProperties;
    }

    /**
     * Consumer监听线程
     * @author dreamyao
     */
    private final class ConsumerListener implements Runnable {

        private final Log logger = LogFactory.getLog(ConsumerListener.class);
        private final ContainerProperties containerProperties = getContainerProperties();
        private final Consumer<K, V> consumer;
        private final MessageComsumer<K, V> messageComsumer;
        private final BatchMessageComsumer<K, V> batchMessageComsumer;

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

                consumer.subscribe(containerProperties.getTopicPattern(), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        if (logger.isTraceEnabled()) {
                            final String msg = String.format("Revoked  %s topic-partitions are revoked from this consumer", Arrays.toString(partitions.toArray()));
                            logger.trace(msg);
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        if (logger.isTraceEnabled()) {
                            final String msg = String.format("Assigned %s topic-partitions are revoked from this consumer", Arrays.toString(partitions.toArray()));
                            logger.trace(msg);
                        }
                    }
                });

            } else {
                consumer.subscribe(Arrays.asList(containerProperties.getTopics()));
            }

            this.consumer = consumer;
            if (genericMessageComsumer instanceof MessageComsumer) {

                this.messageComsumer = (MessageComsumer<K, V>) genericMessageComsumer;
                this.batchMessageComsumer = null;

            } else if (genericMessageComsumer instanceof BatchMessageComsumer) {

                this.messageComsumer = null;
                this.batchMessageComsumer = (BatchMessageComsumer<K, V>) genericMessageComsumer;

            } else {

                this.messageComsumer = null;
                this.batchMessageComsumer = null;
                logger.error("property[genericMessageComsumer] must implements BatchMessageComsumer or MessageComsumer");
            }
        }

        @Override
        public void run() {

            while (isRunning()) {

                try {

                    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(containerProperties.getPollTimeout()));

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
                    logger.error("comsumer message failed.", e);
                }
            }
        }

        /**
         * 消费业务分发
         * @param records
         */
        private void invokeListener(ConsumerRecords<K, V> records) {
            if (batchMessageComsumer != null) {
                invokeBatchRecordListener(records);
            } else if (messageComsumer != null) {
                invokeRecordListener(records);
            }
        }

        /**
         * 批量消费
         * @param records
         */
        private void invokeBatchRecordListener(ConsumerRecords<K, V> records) {
            List<ConsumerRecord<K, V>> recordList = new ArrayList<ConsumerRecord<K, V>>();
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
                e.printStackTrace();
            }
        }

        /**
         * 消费消息
         * @param records
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
                    e.printStackTrace();
                }
            }
        }
    }
}
  