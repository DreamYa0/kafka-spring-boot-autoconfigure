package com.g7.framework.kafka.container;

import com.g7.framework.kafka.comsumer.GenericMessageComsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.util.Assert;

import javax.annotation.concurrent.ThreadSafe;
import java.util.regex.Pattern;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午9:58
 * @since 1.0.0
 */
@ThreadSafe
public class ContainerProperties {

    private static final int DEFAULT_SHUTDOWN_TIMEOUT = 10000;

    private static final int DEFAULT_QUEUE_DEPTH = 1;

    private static final int DEFAULT_PAUSE_AFTER = 10000;

    /**
     * 主题名称
     */
    private String[] topics;

    /**
     * Topic pattern.
     */
    private Pattern topicPattern;

    /**
     * 消息处理器
     */
    private GenericMessageComsumer messageConsumer;

    /**
     * 消费者等待记录时阻止的最长时间。
     */
    private volatile long pollTimeout = 1000;

    /**
     * 轮询消费者的线程的执行者。
     */
    private AsyncTaskExecutor consumerTaskExecutor;

    /**
     * When using Kafka group management and  is
     * true, the delay after which the consumer should be paused. Default 10000.
     */
    private volatile long pauseAfter = DEFAULT_PAUSE_AFTER;

    /**
     * When true, avoids rebalancing when this consumer is slow or throws a
     * qualifying exception - pauses the consumer. Default: true.
     * @see #pauseAfter
     */
    private volatile boolean pauseEnabled = true;

    /**
     * 容器关闭超时时间
     */
    private volatile long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

    /**
     * Set the queue depth for handoffs from the consumer thread to the listener
     * thread. Default 1 (up to 2 in process).
     */
    private volatile int queueDepth = DEFAULT_QUEUE_DEPTH;

    private volatile String groupId;

    /**
     * 创建多少个消费者,默认 1 个
     */
    private volatile int manyComsumer = 1;

    /**
     * 消费者重平衡监听器
     */
    private ConsumerRebalanceListener consumerRebalanceListener;

    private ContainerProperties() {

    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String[] getTopics() {
        return topics;
    }

    public synchronized Pattern getTopicPattern() {
        return topicPattern;
    }

    public long getShutdownTimeout() {
        return shutdownTimeout;
    }

    public int getManyComsumer() {
        return manyComsumer;
    }

    public void setManyComsumer(int manyComsumer) {
        this.manyComsumer = manyComsumer;
    }

    /**
     * 容器关闭超时时间
     * @param shutdownTimeout 容器关闭超时时间
     */
    public void setShutdownTimeout(long shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public synchronized GenericMessageComsumer getMessageConsumer() {
        return messageConsumer;
    }

    public synchronized void setMessageConsumer(GenericMessageComsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    /**
     * Set the max time to block in the consumer waiting for records.
     * @param pollTimeout the timeout in ms; default 1000.
     */
    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public long getPauseAfter() {
        return pauseAfter;
    }

    public boolean isPauseEnabled() {
        return pauseEnabled;
    }

    public void setPauseEnabled(boolean pauseEnabled) {
        this.pauseEnabled = pauseEnabled;
    }

    public int getQueueDepth() {
        return queueDepth;
    }

    /**
     * Set the queue depth for handoffs from the consumer thread to the listener
     * thread. Default 1 (up to 2 in process).
     * @param queueDepth the queue depth.
     */
    public void setQueueDepth(int queueDepth) {
        this.queueDepth = queueDepth;
    }

    public synchronized ConsumerRebalanceListener getConsumerRebalanceListener() {
        return consumerRebalanceListener;
    }

    /**
     * Set the user defined {@link ConsumerRebalanceListener} implementation.
     * @param consumerRebalanceListener the {@link ConsumerRebalanceListener} instance
     */
    public synchronized void setConsumerRebalanceListener(ConsumerRebalanceListener consumerRebalanceListener) {
        this.consumerRebalanceListener = consumerRebalanceListener;
    }

    public AsyncTaskExecutor getConsumerTaskExecutor() {
        return consumerTaskExecutor;
    }

    public void setConsumerTaskExecutor(AsyncTaskExecutor consumerTaskExecutor) {
        this.consumerTaskExecutor = consumerTaskExecutor;
    }

    public void setTopics(String[] topics) {
        this.topics = topics;
    }

    public void setTopicPattern(Pattern topicPattern) {
        this.topicPattern = topicPattern;
    }

    public void setPauseAfter(long pauseAfter) {
        this.pauseAfter = pauseAfter;
    }

    public static ContainerProperties.Builder builder() {
        return new ContainerProperties.Builder();
    }

    public static class Builder {

        private final ContainerProperties obj;

        public Builder() {
            this.obj = new ContainerProperties();
        }

        public Builder topic(String... topics) {
            Assert.notEmpty(topics, "An array of topicPartitions must be provided");
            obj.setTopics(topics);
            obj.setTopicPattern(null);
            return this;
        }

        public Builder topicPattern(Pattern topicPattern) {
            obj.setTopics(null);
            obj.setTopicPattern(topicPattern);
            return this;
        }

        public Builder messageConsumer(GenericMessageComsumer messageConsumer) {
            obj.setMessageConsumer(messageConsumer);
            return this;
        }

        public Builder pollTimeout(final long pollTimeout) {
            obj.setPollTimeout(pollTimeout);
            return this;
        }

        public Builder consumerTaskExecutor(AsyncTaskExecutor consumerTaskExecutor) {
            obj.setConsumerTaskExecutor(consumerTaskExecutor);
            return this;
        }

        public Builder pauseAfter(final long pauseAfter) {
            obj.setPauseAfter(pauseAfter);
            return this;
        }

        public Builder pauseEnabled(final boolean pauseEnabled) {
            obj.setPauseEnabled(pauseEnabled);
            return this;
        }

        public Builder shutdownTimeout(final long shutdownTimeout) {
            obj.setShutdownTimeout(shutdownTimeout);
            return this;
        }

        public Builder queueDepth(final int queueDepth) {
            obj.setQueueDepth(queueDepth);
            return this;
        }

        public Builder groupId(final String groupId) {
            obj.setGroupId(groupId);
            return this;
        }

        public Builder manyComsumer(final int manyComsumer) {
            obj.setManyComsumer(manyComsumer);
            return this;
        }

        public ContainerProperties build() {
            return this.obj;
        }
    }
}
