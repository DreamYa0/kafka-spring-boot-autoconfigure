package com.g7.framework.kafka.factory;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author dreamyao
 * @title kafka producer FactoryBean
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public class ProducerFactoryBean<K, V> implements FactoryBean<Producer<K, V>> {

    private static final Logger logger = LoggerFactory.getLogger(ProducerFactoryBean.class);
    private final Properties properties;

    public ProducerFactoryBean(Properties properties) {
        this.properties = properties;

    }

    @Override
    public Producer<K, V> getObject() throws Exception {
        return new CloseSafeProducer<>(new KafkaProducer<>(properties));
    }

    @Override
    public Class<?> getObjectType() {
        return Producer.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    private static class CloseSafeProducer<K, V> implements Producer<K, V> {

        private final Producer<K, V> delegate;

        CloseSafeProducer(Producer<K, V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void initTransactions() {
            delegate.initTransactions();
        }

        @Override
        public void beginTransaction() throws ProducerFencedException {
            delegate.beginTransaction();
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
                throws ProducerFencedException {
            delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map,
                                             ConsumerGroupMetadata consumerGroupMetadata) throws
                ProducerFencedException {
            delegate.sendOffsetsToTransaction(map, consumerGroupMetadata);
        }

        @Override
        public void commitTransaction() throws ProducerFencedException {
            delegate.commitTransaction();
        }

        @Override
        public void abortTransaction() throws ProducerFencedException {
            delegate.abortTransaction();
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
            return delegate.send(record);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            return delegate.send(record, callback);
        }

        @Override
        public void flush() {
            delegate.flush();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return delegate.partitionsFor(topic);
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return delegate.metrics();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void close(long timeout, TimeUnit unit) {
            delegate.close(timeout, unit);
        }

        @Override
        public void close(Duration duration) {
            delegate.close(duration);
        }
    }
}
  