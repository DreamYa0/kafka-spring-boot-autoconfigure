package com.billionway.framework.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.util.Assert;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Arrays;
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
     * Topic names.
     */
    private final String[] topics;

    /**
     * Topic pattern.
     */
    private final Pattern topicPattern;

    private Object messageConsumer;

    /**
     * The max time to block in the consumer waiting for records.
     */
    private volatile long pollTimeout = 1000;

    /**
     * The executor for threads that poll the consumer.
     */
    private AsyncTaskExecutor consumerTaskExecutor;

    /**
     * When using Kafka group management and {@link #(boolean)} is
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
     * The timeout for shutting down the container. This is the maximum amount of
     * time that the invocation to {@code #stop(Runnable)} will block for, before
     * returning.
     */
    private volatile long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

    /**
     * Set the queue depth for handoffs from the consumer thread to the listener
     * thread. Default 1 (up to 2 in process).
     */
    private volatile int queueDepth = DEFAULT_QUEUE_DEPTH;

    /**
     * A user defined {@link ConsumerRebalanceListener} implementation.
     */
    private ConsumerRebalanceListener consumerRebalanceListener;

    public ContainerProperties(String... topics) {
        Assert.notEmpty(topics, "An array of topicPartitions must be provided");
        this.topics = Arrays.asList(topics).toArray(new String[topics.length]);
        this.topicPattern = null;
    }

    public ContainerProperties(Pattern topicPattern) {
        this.topics = null;
        this.topicPattern = topicPattern;
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

    /**
     * Set the timeout for shutting down the container. This is the maximum amount of
     * time that the invocation to {@code #stop(Runnable)} will block for, before
     * returning.
     * @param shutdownTimeout the shutdown timeout.
     */
    public void setShutdownTimeout(long shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public synchronized Object getMessageConsumer() {
        return messageConsumer;
    }

    public synchronized void setMessageConsumer(Object messageConsumer) {
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
}
