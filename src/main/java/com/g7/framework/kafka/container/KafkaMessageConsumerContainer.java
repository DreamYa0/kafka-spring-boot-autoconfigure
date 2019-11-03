package com.g7.framework.kafka.container;

import com.g7.framework.kafka.comsumer.BatchMessageComsumer;
import com.g7.framework.kafka.comsumer.ConsumerRecordWorker;
import com.g7.framework.kafka.comsumer.GenericMessageComsumer;
import com.g7.framework.kafka.comsumer.SingleMessageComsumer;
import com.g7.framework.kafka.comsumer.SubscribeTypeEnum;
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
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费者容器
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午9:53
 * @since 1.0.0
 */
public class KafkaMessageConsumerContainer<K, V> extends AbstractMessageConsumerContainer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageConsumerContainer.class);

    @Autowired
    private KafkaProperties properties;
    private final List<ConsumerListener> consumerListeners = new CopyOnWriteArrayList<>();
    private List<ConsumerRecordCoordinator> consumerRecordCoordinators = new CopyOnWriteArrayList<>();

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

        setRunning(true);

        SubscribeTypeEnum subscribeType = containerProperties.getSubscribeType();
        if (Objects.isNull(subscribeType)) {
            subscribeType = SubscribeTypeEnum.MANY_CONSUMER_ONE_WORKER;
        }

        switch (subscribeType) {

            case MANY_CONSUMER_MANY_WORKER:

                for (int i = 0; i < containerProperties.getQueueDepth(); i++) {

                    ConsumerRecordCoordinator consumerRecordCoordinator = new ConsumerRecordCoordinator(messageConsumer, groupId, String.format((getBeanName() == null ? "kafkaMessageConsumerContainer" : getBeanName()) + "-%d", i));
                    consumerRecordCoordinators.add(consumerRecordCoordinator);
                    consumerRecordCoordinator.start();
                }

                break;

            case ONE_CONSUMER_MANY_WORKER:

                ConsumerRecordCoordinator consumerRecordCoordinator = new ConsumerRecordCoordinator(messageConsumer, groupId, String.format((getBeanName() == null ? "kafkaMessageConsumerContainer" : getBeanName()) + "-%d", 0));
                consumerRecordCoordinators.add(consumerRecordCoordinator);
                consumerRecordCoordinator.start();

                break;

            case MANY_CONSUMER_ONE_WORKER:
            default:

                // 设置当前消费者组里面消费者的个数
                for (int i = 0; i < containerProperties.getQueueDepth(); i++) {

                    ConsumerListener thread = new ConsumerListener(messageConsumer, groupId, String.format((getBeanName() == null ? "kafkaMessageConsumerContainer" : getBeanName()) + "-%d", i));
                    consumerListeners.add(thread);
                    thread.start();
                }
        }
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

            setRunning(false);

            if (Boolean.FALSE.equals(CollectionUtils.isEmpty(consumerListeners))) {

                for (ConsumerListener thread : consumerListeners) {

                    while (!thread.isFinished()) {

                        // 等待线程完成
                    }

                    logger.info(thread.getName() + " stopped normally for ConsumerListener");
                }
            }

            if (Boolean.FALSE.equals(CollectionUtils.isEmpty(consumerRecordCoordinators))) {

                for (ConsumerRecordCoordinator consumerRecordCoordinator : consumerRecordCoordinators) {

                    while (!consumerRecordCoordinator.isFinished()) {

                        // 等待线程完成
                    }

                    logger.info(consumerRecordCoordinator.getName() + " stopped normally for ConsumerRecordCoordinator");
                }
            }

        } catch (Exception e) {

            logger.error("Error while stopping the container", e);
        }

        if (callback != null) {
            callback.run();
        }
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
    private final class ConsumerListener extends Thread {

        private final Logger logger = LoggerFactory.getLogger(ConsumerListener.class);
        private final ContainerProperties containerProperties = getContainerProperties();
        private final Consumer<K, V> consumer;
        private final SingleMessageComsumer<K, V> singleMessageComsumer;
        private final BatchMessageComsumer<K, V> batchMessageComsumer;
        private final KafkaConsumerFactory<K, V> consumerFactory = createKafkaConsumerFactory();
        /**
         * 线程是否完成
         */
        private final AtomicBoolean isFinish = new AtomicBoolean(false);

        @SuppressWarnings("unchecked")
        private ConsumerListener(GenericMessageComsumer genericMessageComsumer, String groupId, String threadName) {

            super(threadName);
            this.consumer = new ConsumerBuilder().build(groupId);

            if (genericMessageComsumer instanceof SingleMessageComsumer) {

                singleMessageComsumer = (SingleMessageComsumer<K, V>) genericMessageComsumer;
                batchMessageComsumer = null;

            } else if (genericMessageComsumer instanceof BatchMessageComsumer) {

                singleMessageComsumer = null;
                batchMessageComsumer = (BatchMessageComsumer<K, V>) genericMessageComsumer;

            } else {

                singleMessageComsumer = null;
                batchMessageComsumer = null;

                logger.error("GenericMessageComsumer must implements BatchMessageComsumer or MessageComsumer");
            }
        }

        @Override
        public void run() {

            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {

                logger.error("ConsumerListener thread interrupt , thread again starting...", e);

                runTask();
            });

            runTask();
        }

        private void runTask() {

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
                    if (consumerFactory.isAutoCommit()) {
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

                    logger.error("ConsumerListener comsumer message failed.", e);
                }
            }

            consumer.close();
            isFinish.compareAndSet(false, true);
        }

        private boolean isFinished() {
            return isFinish.get();
        }

        /**
         * 消费业务分发
         * @param records kafka 消息记录
         */
        private void invokeListener(ConsumerRecords<K, V> records) {

            if (batchMessageComsumer != null) {

                invokeBatchRecordListener(records);

            } else if (singleMessageComsumer != null) {

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
                    singleMessageComsumer.onMessage(record);
                } catch (Exception e) {
                    logger.error("Consumer message failed , consumer name is {}", singleMessageComsumer.getClass().getName(), e);
                }
            }
        }
    }

    private final class ConsumerBuilder {

        private final ContainerProperties containerProperties = getContainerProperties();
        private final KafkaConsumerFactory<K, V> consumerFactory = createKafkaConsumerFactory();

        private Consumer<K, V> build(String groupId) {

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

            return consumer;
        }
    }

    private final class ConsumerRecordCoordinator extends Thread {

        private final Logger logger = LoggerFactory.getLogger(ConsumerRecordCoordinator.class);
        private final ContainerProperties containerProperties = getContainerProperties();
        private final KafkaConsumerFactory<K, V> consumerFactory = createKafkaConsumerFactory();
        private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
        /**
         * 线程是否完成
         */
        private final AtomicBoolean isFinish = new AtomicBoolean(false);
        private final GenericMessageComsumer genericMessageComsumer;
        private final String groupId;

        private ConsumerRecordCoordinator(GenericMessageComsumer genericMessageComsumer, String groupId, String threadName) {
            super(threadName);
            this.genericMessageComsumer = genericMessageComsumer;
            this.groupId = groupId;
        }

        @Override
        public void run() {

            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {

                logger.error("ConsumerRecordCoordinator thread interrupt , thread again starting...", e);

                coordinate();
            });

            coordinate();
        }

        private void coordinate() {

            Consumer<K, V> consumer = new ConsumerBuilder().build(groupId);

            // 异步线程池
            SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor(new ThreadFactoryBuilder()
                    .setNameFormat((getBeanName() == null ? "kafkaMessageConsumerContainer" : getBeanName())+"-consumerRecordWorker" + "-%d")
                    .build());
            // 最大并发限流设置
            asyncTaskExecutor.setConcurrencyLimit(containerProperties.getQueueDepth());

            while (isRunning()) {

                try {

                    ConsumerRecords<K, V> records = consumer.poll(containerProperties.getPollTimeout());
                    if (Boolean.FALSE.equals(records.isEmpty())) {
                        asyncTaskExecutor.submit(new ConsumerRecordWorker<>(records, offsets, genericMessageComsumer));
                    }

                    // 判断是否自动提交
                    if (consumerFactory.isAutoCommit()) {
                        continue;
                    }

                    commitOffsets(consumer);

                } catch (Exception e) {
                    logger.error("ConsumerRecordCoordinator consumer message failed.", e);
                }
            }

            consumer.close();
            isFinish.compareAndSet(false, true);
        }

        private boolean isFinished() {
            return isFinish.get();
        }

        private void commitOffsets(Consumer<K, V> consumer) {

            final Map<TopicPartition, OffsetAndMetadata> unmodfiedMap;

            synchronized (offsets) {

                if (offsets.isEmpty()) {
                    return;
                }

                unmodfiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
                offsets.clear();
            }

            consumer.commitSync(unmodfiedMap);
        }
    }
}
  