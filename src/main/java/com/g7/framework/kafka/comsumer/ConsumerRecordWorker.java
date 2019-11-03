package com.g7.framework.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * @author dreamyao
 * @title worker thread 用于处理业务逻辑
 * @date 2018/8/7 下午2:49
 * @since 1.0.0
 */
public class ConsumerRecordWorker<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerRecordWorker.class);
    private final ConsumerRecords<K, V> records;
    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsets;
    private final GenericMessageComsumer genericMessageComsumer;


    public ConsumerRecordWorker(ConsumerRecords<K, V> records, ConcurrentMap<TopicPartition, OffsetAndMetadata> offsets, GenericMessageComsumer genericMessageComsumer) {
        this.records = records;
        this.offsets = offsets;
        this.genericMessageComsumer = genericMessageComsumer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {

        for (TopicPartition partition : records.partitions()) {

            try {

                List<ConsumerRecord<K, V>> consumerRecords = records.records(partition);

                if (CollectionUtils.isEmpty(consumerRecords)) {
                    return;
                }

                if (genericMessageComsumer instanceof SingleMessageComsumer) {

                    SingleMessageComsumer<K, V> singleMessageComsumer = (SingleMessageComsumer<K, V>) genericMessageComsumer;

                    for (ConsumerRecord<K, V> record : consumerRecords) {

                        singleMessageComsumer.onMessage(record);

                    }

                } else {

                    BatchMessageComsumer<K, V> batchMessageComsumer = (BatchMessageComsumer<K, V>) genericMessageComsumer;
                    batchMessageComsumer.onMessage(consumerRecords);

                }

                /* 同步确认某个分区的特定offset */
                long lastOffset = consumerRecords.get(consumerRecords.size() - 1).offset();

                if (Boolean.FALSE.equals(offsets.containsKey(partition))) {

                    offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));

                } else {

                    long offset = offsets.get(partition).offset();
                    if (offset <= lastOffset + 1) {
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }
                }

            } catch (Exception e) {
                logger.error("Consumer message error.", e);
            }
        }
    }
}