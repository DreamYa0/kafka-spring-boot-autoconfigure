package com.g7.framework.kafka.container;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.g7.framework.kafka.comsumer.BatchMessageConsumer;
import com.g7.framework.kafka.comsumer.ConsumerModeEnum;
import com.g7.framework.kafka.comsumer.ConsumerRecordWorker;
import com.g7.framework.kafka.comsumer.GenericMessageComsumer;
import com.g7.framework.kafka.comsumer.SingleMessageComsumer;
import com.g7.framework.kafka.factory.KafkaConsumerFactory;
import com.g7.framework.kafka.properties.ContainerProperties;
import com.g7.framework.kafka.properties.KafkaProperties;
import com.g7.framework.kafka.util.ReadPropertiesUtils;
import com.g7.framework.trace.SpanContext;
import com.g7.framework.trace.TraceContext;
import com.g7.framework.trace.thread.ThreadPoolTaskExecutorMdcWrapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.g7.framework.trace.Constants.*;

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
    private ThreadPoolTaskExecutor consumerRecordWorkerExecutor;

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

        ConsumerModeEnum consumerMode = containerProperties.getConsumerMode();
        if (Objects.isNull(consumerMode)) {
            consumerMode = ConsumerModeEnum.MANY_CONSUMER_ONE_WORKER;
        }

        switch (consumerMode) {

            case MANY_CONSUMER_MANY_WORKER:

                // 创建Worker线程池
                createThreadPool();

                for (int i = 0; i < containerProperties.getQueueDepth(); i++) {

                    ConsumerRecordCoordinator consumerRecordCoordinator = new ConsumerRecordCoordinator(messageConsumer,
                            groupId, String.format(
                            (getBeanName() == null ? "kafkaMessageConsumerContainer" : getBeanName()) + "-%d", i));
                    consumerRecordCoordinators.add(consumerRecordCoordinator);
                    consumerRecordCoordinator.start();
                }

                break;

            case ONE_CONSUMER_MANY_WORKER:

                // 创建Worker线程池
                createThreadPool();

                ConsumerRecordCoordinator consumerRecordCoordinator = new ConsumerRecordCoordinator(messageConsumer,
                        groupId, String.format(
                        (getBeanName() == null ? "kafkaMessageConsumerContainer" : getBeanName()) + "-%d", 0));
                consumerRecordCoordinators.add(consumerRecordCoordinator);
                consumerRecordCoordinator.start();

                break;

            case MANY_CONSUMER_ONE_WORKER:
            default:

                // 设置当前消费者组里面消费者的个数
                for (int i = 0; i < containerProperties.getQueueDepth(); i++) {

                    ConsumerListener thread = new ConsumerListener(messageConsumer,
                            groupId, String.format(
                            (getBeanName() == null ? "kafkaMessageConsumerContainer" : getBeanName()) + "-%d", i));
                    consumerListeners.add(thread);
                    thread.start();
                }
        }
    }

    private void createThreadPool() {

        final int cpuCount = Runtime.getRuntime().availableProcessors();

        ThreadPoolTaskExecutor wrapper = new ThreadPoolTaskExecutorMdcWrapper();
        wrapper.setCorePoolSize(cpuCount + 1);
        wrapper.setMaxPoolSize(cpuCount * 2 + 1);
        wrapper.setKeepAliveSeconds(60);
        wrapper.setQueueCapacity(500);
        wrapper.setThreadFactory(new ThreadFactoryBuilder()
                .setNameFormat((getBeanName() == null ? "kafkaMessageConsumerContainer" : getBeanName()) +
                        "-consumerRecordWorker" + "-%d")
                .build());
        wrapper.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        wrapper.initialize();

        consumerRecordWorkerExecutor = wrapper;
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

            // 关闭多Consumer模式的Consumer线程
            if (Boolean.FALSE.equals(CollectionUtils.isEmpty(consumerListeners))) {

                for (ConsumerListener thread : consumerListeners) {

                    while (!thread.isFinished()) {

                        // 等待线程完成
                    }

                    logger.info(thread.getName() + " stopped normally for ConsumerListener");
                }
            }

            // 关闭Consumer多Worker模式的协调器
            if (Boolean.FALSE.equals(CollectionUtils.isEmpty(consumerRecordCoordinators))) {

                for (ConsumerRecordCoordinator consumerRecordCoordinator : consumerRecordCoordinators) {

                    while (!consumerRecordCoordinator.isFinished()) {

                        // 等待线程完成
                    }

                    logger.info(consumerRecordCoordinator.getName() + " stopped normally for ConsumerRecordCoordinator");
                }
            }

            // 关闭Worker线程池
            if (Objects.nonNull(consumerRecordWorkerExecutor)) {
                consumerRecordWorkerExecutor.shutdown();
                logger.info("ConsumerWorker thread pool stopped normally");
            }

        } catch (Exception e) {

            logger.error("Error while stopping the container", e);
        }

        if (callback != null) {
            callback.run();
        }
    }

    private void generateTrace() {
        final String traceId = TraceContext.getContext().genTraceIdAndSet();
        MDC.put(MDC_TRACE_NAME, traceId);
        MDC.put(TRACE_ID, traceId);
        final String spanId = SpanContext.getContext().genSpanIdAndSet();
        MDC.put(MDC_SPAN_NAME, spanId);
        MDC.put(SPAN_ID, spanId);
    }

    private void removeTrace() {
        TraceContext.removeContext();
        SpanContext.removeContext();
    }

    /**
     * Consumer监听线程
     * @author dreamyao
     */
    private final class ConsumerListener extends Thread {

        private final Logger logger = LoggerFactory.getLogger(ConsumerListener.class);
        private final ContainerProperties containerProperties = getContainerProperties();
        private final SingleMessageComsumer<K, V> singleMessageComsumer;
        private final BatchMessageConsumer<K, V> batchMessageConsumer;
        private final ConsumerBuilder consumerBuilder = new ConsumerBuilder();
        private final String groupId;
        /**
         * 线程是否完成
         */
        private final AtomicBoolean isFinish = new AtomicBoolean(false);

        @SuppressWarnings("unchecked")
        private ConsumerListener(GenericMessageComsumer genericMessageComsumer, String groupId, String threadName) {

            super(threadName);
            this.groupId = groupId;

            if (genericMessageComsumer instanceof SingleMessageComsumer) {

                singleMessageComsumer = (SingleMessageComsumer<K, V>) genericMessageComsumer;
                batchMessageConsumer = null;

            } else if (genericMessageComsumer instanceof BatchMessageConsumer) {

                singleMessageComsumer = null;
                batchMessageConsumer = (BatchMessageConsumer<K, V>) genericMessageComsumer;

            } else {

                singleMessageComsumer = null;
                batchMessageConsumer = null;

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

            Consumer<K, V> consumer = consumerBuilder.build(groupId);

            while (isRunning()) {

                try {

                    generateTrace();

                    ConsumerRecords<K, V> records = consumer.poll(containerProperties.getPollTimeout());

                    if (records != null && logger.isDebugEnabled()) {
                        logger.debug("Received: " + records.count() + " records");
                    }

                    if (records != null && records.count() > 0) {
                        invokeListener(records);
                    }

                    // 判断是否自动提交
                    if (consumerBuilder.isAutoCommit()) {
                        continue;
                    }

                    // 提交partitions
                    if (Objects.nonNull(records)) {

                        for (TopicPartition partition : records.partitions()) {

                            List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);

                            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

                            consumer.commitSync(Collections.singletonMap(partition,
                                    new OffsetAndMetadata(lastOffset + 1)));
                        }
                    }

                } catch (Exception e) {

                    logger.error("ConsumerListener comsumer message failed.", e);

                } finally {

                    removeTrace();
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

            if (batchMessageConsumer != null) {
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

            String topic = recordList.get(0).topic();
            Transaction transaction = Cat.newTransaction("KafkaBatchConsumer", topic);

            try {

                batchMessageConsumer.onMessage(recordList);

                transaction.setStatus(Transaction.SUCCESS);

            } catch (Exception e) {

                Cat.logErrorWithCategory("KafkaBatchConsumer", "Consumer batch message failed",
                        e);
                logger.error("Consumer batch message failed , consumer name is {}",
                             batchMessageConsumer.getClass().getName(), e);
                throw e;

            } finally {
                transaction.complete();
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

                String topic = record.topic();
                Transaction transaction = Cat.newTransaction("KafkaSingleConsumer", topic);

                try {

                    singleMessageComsumer.onMessage(record);

                    transaction.setStatus(Transaction.SUCCESS);

                } catch (Exception e) {

                    Cat.logErrorWithCategory("KafkaSingleConsumer", "Consumer single message failed",
                            e);
                    logger.error("Consumer single message failed , consumer name is {}",
                            singleMessageComsumer.getClass().getName(), e);
                    throw e;
                } finally {
                    transaction.complete();
                }
            }
        }
    }

    private final class ConsumerRecordCoordinator extends Thread {

        private final Logger logger = LoggerFactory.getLogger(ConsumerRecordCoordinator.class);
        private final ContainerProperties containerProperties = getContainerProperties();
        private final ConsumerBuilder consumerBuilder = new ConsumerBuilder();
        private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
        private final String groupId;
        /**
         * 线程是否完成
         */
        private final AtomicBoolean isFinish = new AtomicBoolean(false);
        private final GenericMessageComsumer genericMessageComsumer;

        private ConsumerRecordCoordinator(GenericMessageComsumer genericMessageComsumer, String groupId,
                                          String threadName) {
            super(threadName);
            this.groupId = groupId;
            this.genericMessageComsumer = genericMessageComsumer;
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

            Consumer<K, V> consumer = consumerBuilder.build(groupId);

            while (isRunning()) {

                try {

                    generateTrace();

                    ConsumerRecords<K, V> records = consumer.poll(containerProperties.getPollTimeout());
                    if (Boolean.FALSE.equals(records.isEmpty())) {
                        consumerRecordWorkerExecutor.execute(
                                new ConsumerRecordWorker<>(records, offsets,
                                        genericMessageComsumer));
                    }

                    // 判断是否自动提交
                    if (consumerBuilder.isAutoCommit()) {
                        continue;
                    }

                    commitOffsets(consumer);

                } catch (Exception e) {
                    logger.error("ConsumerRecordCoordinator consumer message failed.", e);

                } finally {

                    removeTrace();
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

    private final class ConsumerBuilder {

        private final ContainerProperties containerProperties = getContainerProperties();
        private final KafkaConsumerFactory<K, V> consumerFactory;

        private ConsumerBuilder() {
            this.consumerFactory = createKafkaConsumerFactory();
        }

        private Consumer<K, V> build(String groupId) {

            final Consumer<K, V> consumer;

            if (StringUtils.isEmpty(groupId)) {
                consumer = consumerFactory.createConsumer();
            } else {
                consumer = consumerFactory.createConsumer4Group(groupId);
            }

            if (containerProperties.getTopicPattern() != null) {
                consumer.subscribe(containerProperties.getTopicPattern(),
                        containerProperties.getConsumerRebalanceListener());
            } else {
                consumer.subscribe(Arrays.asList(containerProperties.getTopics()),
                        containerProperties.getConsumerRebalanceListener());
            }

            return consumer;
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

        private boolean isAutoCommit() {
            return consumerFactory.isAutoCommit();
        }
    }
}
  