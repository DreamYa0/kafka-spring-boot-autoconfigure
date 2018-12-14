package io.seg.framework.message.factory;

import io.seg.framework.message.codec.MessageDecoder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Properties;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public class KafkaConsumerFactory<K, V> {

    private final Properties configs;
    private Deserializer<K> keyDeserializer=new MessageDecoder<>();
    private Deserializer<V> valueDeserializer=new MessageDecoder<>();

    public KafkaConsumerFactory(Properties configs) {
        this.configs = configs;
    }

    public void setKeyDeserializer(Deserializer<K> keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public void setValueDeserializer(Deserializer<V> valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    public Consumer<K, V> createConsumer() {
        return createKafkaConsumer();
    }

    public Consumer<K, V> createConsumer(String clientIdSuffix) {
        return createKafkaConsumer(clientIdSuffix);
    }

    private KafkaConsumer<K, V> createKafkaConsumer() {
        return createKafkaConsumer(configs);
    }

    private KafkaConsumer<K, V> createKafkaConsumer(String clientIdSuffix) {
        if (!this.configs.containsKey(ConsumerConfig.CLIENT_ID_CONFIG) || clientIdSuffix == null) {
            return createKafkaConsumer();
        } else {
            Properties modifiedClientIdConfigs = configs;
            modifiedClientIdConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG,
                    modifiedClientIdConfigs.get(ConsumerConfig.CLIENT_ID_CONFIG) + clientIdSuffix);
            return createKafkaConsumer(modifiedClientIdConfigs);
        }
    }

    private KafkaConsumer<K, V> createKafkaConsumer(Properties configs) {
        return new KafkaConsumer<>(configs, keyDeserializer, valueDeserializer);
    }

    public boolean isAutoCommit() {
        Object auto = this.configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        return auto instanceof Boolean ? (Boolean) auto
                : auto instanceof String ? Boolean.valueOf((String) auto) : true;
    }
}
  