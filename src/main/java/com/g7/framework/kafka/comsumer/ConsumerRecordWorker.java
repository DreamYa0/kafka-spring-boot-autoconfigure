package com.g7.framework.kafka.comsumer;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
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


    public ConsumerRecordWorker(ConsumerRecords<K, V> records,
                                ConcurrentMap<TopicPartition, OffsetAndMetadata> offsets,
                                GenericMessageComsumer genericMessageComsumer) {
        this.records = records;
        this.offsets = offsets;
        this.genericMessageComsumer = genericMessageComsumer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {

        for (TopicPartition partition : records.partitions()) {

            String topic = partition.topic();
            Transaction transaction = Cat.newTransaction("ConsumerRecordWorker", topic);

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

                    BatchMessageConsumer<K, V> batchMessageConsumer = (BatchMessageConsumer<K, V>) genericMessageComsumer;
                    batchMessageConsumer.onMessage(consumerRecords);

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

                transaction.setStatus(Transaction.SUCCESS);

            } catch (Exception e) {

                Cat.logErrorWithCategory("ConsumerRecordWorker", "Consumer record worker error.", e);
                logger.error("Consumer message error , consumer name is [{}]",
                        genericMessageComsumer.getClass().getName(), e);

            } finally {
                transaction.complete();
            }
        }
    }
}
