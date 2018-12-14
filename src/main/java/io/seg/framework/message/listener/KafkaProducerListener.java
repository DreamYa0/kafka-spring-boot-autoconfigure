package io.seg.framework.message.listener;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Producer监听器
 * @author dreamyao
 * @version V1.0
 * @date 2017年5月15日 下午2:09:24
 */
public class KafkaProducerListener implements ProducerListener<String, Object> {

    protected final Logger logger = LoggerFactory.getLogger(KafkaProducerListener.class);

    @Override
    public boolean isInterestedInSuccess() {
        return true;
    }

    @Override
    public void onSuccess(String topic, Integer partition, String key, Object value, RecordMetadata recordMetadata) {
        logger.info("send message success. topic: %s, partition: %d, key: %s, value: %s， recordMetadata: %s", topic, partition, key, value, recordMetadata);
    }

    @Override
    public void onError(String topic, Integer partition, String key, Object value, Exception exception) {
        logger.error("send message fail. topic: %s, partition: %d, key: %s, value: %s", topic, partition, key, value);
        logger.error(exception.getMessage(), exception);
    }

}
  