package com.g7.framework.kafka.container;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public class KafkaConsumerFactory<K, V> {

    private final Properties configs;

    public KafkaConsumerFactory(Properties configs) {
        this.configs = configs;
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
        return new KafkaConsumer<>(configs);
    }

    public boolean isAutoCommit() {
        Object auto = this.configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        return auto instanceof Boolean ? (Boolean) auto
                : auto instanceof String ? Boolean.valueOf((String) auto) : true;
    }
}
  