package com.g7.framework.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ObjectUtils;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public class LoggingProducerListener<K, V> extends ProducerListenerAdapter<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(LoggingProducerListener.class);
    private volatile boolean includeContents = true;
    private volatile int maxContentLogged = 100;

    /**
     * 日志消息是否应包含内容（密钥和有效负载）
     * @param includeContents 如果应该记录消息的内容，则为true
     */
    public void setIncludeContents(boolean includeContents) {
        this.includeContents = includeContents;
    }

    /**
     * 密钥或密码要记录的最大数据量。由于消息大小可能会变化并变得相当大，因此可以限制发送到日志的数据量
     * @param maxContentLogged 正在记录的最大数据量
     */
    public void setMaxContentLogged(int maxContentLogged) {
        this.maxContentLogged = maxContentLogged;
    }

    @Override
    public void onError(String topic, Integer partition, K key, V value, Exception exception) {

        if (logger.isErrorEnabled()) {

            StringBuilder logOutput = new StringBuilder();
            logOutput.append("Exception thrown when sending a message");

            if (includeContents) {

                logOutput.append(" with key='").append(toDisplayString(ObjectUtils.nullSafeToString(key), maxContentLogged)).append("'");
                logOutput.append(" and payload='").append(toDisplayString(ObjectUtils.nullSafeToString(value), maxContentLogged)).append("'");
            }

            logOutput.append(" to topic ").append(topic);

            if (partition != null) {
                logOutput.append(" and partition ").append(partition);
            }

            logOutput.append(":");
            logger.error(logOutput.toString(), exception);
        }
    }

    private String toDisplayString(String original, int maxCharacters) {

        if (original.length() <= maxCharacters) {
            return original;
        }
        return original.substring(0, maxCharacters) + "...";
    }
}
  