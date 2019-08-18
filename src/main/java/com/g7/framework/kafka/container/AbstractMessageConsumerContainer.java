package com.g7.framework.kafka.container;

import com.g7.framework.kafka.comsumer.Comsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 抽象消费者容器
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午9:53
 * @since 1.0.0
 */
public abstract class AbstractMessageConsumerContainer implements BeanNameAware, ApplicationEventPublisherAware, SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(AbstractMessageConsumerContainer.class);
    private final ContainerProperties containerProperties;
    private final Object lifecycleMonitor = new Object();
    private String beanName;
    private ApplicationEventPublisher applicationEventPublisher;
    private volatile boolean autoStartup = true;
    private volatile int phase = 0;
    private volatile boolean running = false;

    protected AbstractMessageConsumerContainer(ContainerProperties containerProperties) {

        Assert.notNull(containerProperties, "'containerProperties' cannot be null");

        if (containerProperties.getTopics() != null) {

            this.containerProperties = ContainerProperties.builder().topic(containerProperties.getTopics()).build();

        } else if (containerProperties.getTopicPattern() != null) {

            this.containerProperties = ContainerProperties.builder().topicPattern(containerProperties.getTopicPattern()).build();
        } else {
            this.containerProperties = ContainerProperties.builder().build();
        }

        BeanUtils.copyProperties(containerProperties, this.containerProperties, "topics", "topicPartitions", "topicPattern", "ackCount", "ackTime");

        if (this.containerProperties.getConsumerRebalanceListener() == null) {
            this.containerProperties.setConsumerRebalanceListener(createConsumerRebalanceListener());
        }
    }

    protected String getBeanName() {
        return this.beanName;
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    public ApplicationEventPublisher getApplicationEventPublisher() {
        return this.applicationEventPublisher;
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public boolean isAutoStartup() {
        return this.autoStartup;
    }

    public void setAutoStartup(boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    protected void setRunning(boolean running) {
        this.running = running;
    }

    @Override
    public int getPhase() {
        return this.phase;
    }

    public void setPhase(int phase) {
        this.phase = phase;
    }

    public ContainerProperties getContainerProperties() {
        return this.containerProperties;
    }

    @Override
    public final void start() {
        synchronized (this.lifecycleMonitor) {
            Assert.isTrue(containerProperties.getMessageConsumer() != null, "A " + Comsumer.class.getName() + " implementation must be provided");
            doStart();
        }
    }

    protected abstract void doStart();

    @Override
    public final void stop() {

        final CountDownLatch latch = new CountDownLatch(1);
        stop(latch::countDown);

        try {

            latch.await(containerProperties.getShutdownTimeout(), TimeUnit.MILLISECONDS);

        } catch (InterruptedException e) {

            logger.error("stop consumer container failed.", e);
        }
    }

    @Override
    public void stop(Runnable callback) {

        synchronized (lifecycleMonitor) {
            doStop(callback);
        }
    }

    protected abstract void doStop(Runnable callback);

    /**
     * 创建kafka消息者组重平衡监听器
     * @return the {@link ConsumerRebalanceListener} currently assigned to this container.
     */
    private ConsumerRebalanceListener createConsumerRebalanceListener() {

        return new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.info("partitions revoked:" + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.info("partitions assigned:" + partitions);
            }

        };
    }
}
  