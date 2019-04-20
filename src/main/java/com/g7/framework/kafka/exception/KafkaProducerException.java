package com.g7.framework.kafka.exception;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public class KafkaProducerException extends KafkaException {

    private static final long serialVersionUID = -6174329600243891300L;

    private final ProducerRecord<?, ?> producerRecord;

    public KafkaProducerException(ProducerRecord<?, ?> failedProducerRecord, String message, Throwable cause) {
        super(message, cause);
        this.producerRecord = failedProducerRecord;
    }

    public ProducerRecord<?, ?> getProducerRecord() {
        return this.producerRecord;
    }
}
  