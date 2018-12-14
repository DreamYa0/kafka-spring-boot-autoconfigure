package com.billionway.framework.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * @author dreamyao
 * @title worker thread 用于处理业务逻辑
 * @date 2018/8/7 下午2:49
 * @since 1.0.0
 */
public class ConsumerWorker<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private final ConsumerRecords<K, V> records;
    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsets;
    private final EventCallback<V> eventCallback;

    protected ConsumerWorker(ConsumerRecords<K, V> records, ConcurrentMap<TopicPartition, OffsetAndMetadata> offsets, EventCallback<V> eventCallback) {
        this.records = records;
        this.offsets = offsets;
        this.eventCallback = eventCallback;
    }

    @Override
    public void run() {
        for (TopicPartition partition : records.partitions()) {

            try {
                List<ConsumerRecord<K, V>> consumerRecords = records.records(partition);
                for (ConsumerRecord<K, V> record : consumerRecords) {
                    V value = record.value();
                    int retry = 3;
                    long start = System.currentTimeMillis();
                    while (retry > 0) {
                        try {
                            eventCallback.consume(value);
                            break;
                        } catch (KafkaException e) {
                            logger.error("callback handle exception , remaining retries {}", retry, e);
                        }
                        retry--;
                        // 延迟300ms
                        Thread.sleep(300);
                    }
                    long consumerEnd = System.currentTimeMillis();
                    logger.info("-------- Topic name: {} , consumer event time: {} ms --------", record.topic(), consumerEnd - start);
                }
                /* 同步确认某个分区的特定offset */
                long lastOffset = consumerRecords.get(consumerRecords.size() - 1).offset();

                if (Boolean.FALSE.equals(offsets.containsKey(partition))) {
                    offsets.put(partition, new OffsetAndMetadata(lastOffset));
                } else {
                    long offset = offsets.get(partition).offset();
                    if (offset <= lastOffset + 1) {
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }
                }

            } catch (Exception e) {
                logger.error("consumer message error.", e);
            }
        }
    }
}
