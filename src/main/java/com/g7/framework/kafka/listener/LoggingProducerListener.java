package com.g7.framework.kafka.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.ObjectUtils;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public class LoggingProducerListener<K, V> extends ProducerListenerAdapter<K, V> {

    private static final Log log = LogFactory.getLog(LoggingProducerListener.class);
    private volatile boolean includeContents = true;
    private volatile int maxContentLogged = 100;

    /**
     * Whether the log message should include the contents (key and payload).
     * @param includeContents true if the contents of the message should be logged
     */
    public void setIncludeContents(boolean includeContents) {
        this.includeContents = includeContents;
    }

    /**
     * The maximum amount of data to be logged for either key or password. As message sizes may vary and
     * become fairly large, this allows limiting the amount of data sent to logs.
     * @param maxContentLogged the maximum amount of data being logged.
     */
    public void setMaxContentLogged(int maxContentLogged) {
        this.maxContentLogged = maxContentLogged;
    }

    @Override
    public void onError(String topic, Integer partition, K key, V value, Exception exception) {
        if (log.isErrorEnabled()) {
            StringBuffer logOutput = new StringBuffer();
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
            log.error(logOutput, exception);
        }
    }

    private String toDisplayString(String original, int maxCharacters) {
        if (original.length() <= maxCharacters) {
            return original;
        }
        return original.substring(0, maxCharacters) + "...";
    }

}
  