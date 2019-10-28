package com.g7.framework.kafka.schema;

import com.g7.framework.kafka.factory.ProducerFactoryBean;
import com.g7.framework.kafka.producer.KafkaTemplate;
import com.g7.framework.kafka.producer.KafkaTransactionTemplate;
import com.g7.framework.kafka.properties.KafkaProperties;
import com.g7.framework.kafka.util.ReadPropertiesUtils;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

import java.util.Properties;
import java.util.Random;

/**
 * @author dreamyao
 * @title
 * @date 2018/8/5 上午11:39
 * @since 1.0.0
 */
@EnableConfigurationProperties(KafkaProperties.class)
public class EventMessageAutoConfiguration {

    private final KafkaProperties properties;

    public EventMessageAutoConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean(name = "producer")
    public <K, V> ProducerFactoryBean<K, V> producer() {

        Properties defaultProducerProperties = buildProducerProperties();

        return new ProducerFactoryBean<>(defaultProducerProperties);
    }

    @Bean(name = "transactionProducer")
    public <K, V> ProducerFactoryBean<K, V> transactionProducer() {

        Properties defaultProducerProperties = buildProducerProperties();
        Random rand = new Random();
        // 随机产生一个事物ID，保证同一个机器唯一即可
        defaultProducerProperties.setProperty("transactional.id", String.valueOf(rand.nextInt(999999)));

        return new ProducerFactoryBean<>(defaultProducerProperties);
    }

    private Properties buildProducerProperties() {

        Properties defaultProducerProperties = ReadPropertiesUtils.readProducerDefaultProperties();
        defaultProducerProperties.setProperty("bootstrap.servers", properties.getBootstrap().getServers());

        String keySerializer = properties.getProducer().getKeySerializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(keySerializer))) {
            defaultProducerProperties.setProperty("key.serializer", keySerializer);
        }

        String valueSerializer = properties.getProducer().getValueSerializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(valueSerializer))) {
            defaultProducerProperties.setProperty("value.serializer", valueSerializer);
        }
        return defaultProducerProperties;
    }

    @Bean
    @ConditionalOnMissingBean(value = KafkaTemplate.class)
    public <K, V> KafkaTemplate<K, V> kafkaTemplate(@Autowired @Qualifier(value = "producer") Producer<K, V> producer) {
        return new KafkaTemplate<>(producer);
    }

    @Bean
    @ConditionalOnMissingBean(value = KafkaTransactionTemplate.class)
    public <K, V> KafkaTransactionTemplate<K, V> kvKafkaTransactionTemplate(@Autowired @Qualifier(value = "transactionProducer") Producer<K, V> producer) {
        return new KafkaTransactionTemplate<>(producer);
    }
}
