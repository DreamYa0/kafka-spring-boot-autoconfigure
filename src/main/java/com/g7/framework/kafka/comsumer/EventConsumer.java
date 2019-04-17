package com.g7.framework.kafka.comsumer;

import com.g7.framework.kafka.factory.SpringExtensionFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:02
 * @since 1.0.0
 */
public class EventConsumer<T> implements ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware, DisposableBean, BeanNameAware {

    private static final Logger logger = LoggerFactory.getLogger(EventConsumer.class);
    private Properties properties;
    private ApplicationContext applicationContext;
    private ExecutorService service;
    private String name;

    public EventConsumer(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        Objects.requireNonNull(properties, "properties should not be null");
        Map<String, EventCallback> beansOfType = applicationContext.getBeansOfType(EventCallback.class);
        int size = beansOfType.values().size();
        if (size > 0) {
            service = Executors.newFixedThreadPool(size, new ThreadFactoryBuilder().setNameFormat("comsumer-task-%d: ").build());
            for (EventCallback eventCallback : beansOfType.values()) {
                service.submit(new KafkaConsumerRunner(eventCallback));
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        if (Objects.nonNull(applicationContext)) {
            SpringExtensionFactory.addApplicationContext(applicationContext);
        }
    }

    @Override
    public void setBeanName(String name) {
        this.name = name;
    }

    @Override
    public void destroy() throws Exception {
        if (Objects.nonNull(service) && Boolean.FALSE.equals(service.isShutdown())) {
            service.shutdown();
        }
    }

    private class KafkaConsumerRunner implements Runnable {

        private final EventCallback eventCallback;
        private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();

        private KafkaConsumerRunner(EventCallback eventCallback) {
            this.eventCallback = eventCallback;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {

            String topic = eventCallback.getTopic();
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
            KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    consumer.commitSync(offsets);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    offsets.clear();
                }
            });

            final int cpuCount = Runtime.getRuntime().availableProcessors();

            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    cpuCount / 2,
                    cpuCount * 2,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());

            try {

                while (true) {

                    ConsumerRecords<String, Object> records = consumer.poll(100);
                    if (Boolean.FALSE.equals(records.isEmpty())) {
                        executor.submit(new ConsumerWorker<>(records, offsets, eventCallback));
                    }
                    commitOffsets(consumer);
                }
            } catch (WakeupException e) {
                // 不处理此异常
            } finally {
                commitOffsets(consumer);
                consumer.close();
                executor.shutdown();
            }
        }

        private void commitOffsets(KafkaConsumer<String, Object> consumer) {
            Map<TopicPartition, OffsetAndMetadata> unmodfiedMap;
            if (offsets.isEmpty()) {
                return;
            }
            unmodfiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            offsets.clear();
            consumer.commitSync(unmodfiedMap);
        }
    }
}
