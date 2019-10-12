package com.g7.framework.kafka.schema;

import com.g7.framework.kafka.factory.ProducerFactoryBean;
import com.g7.framework.kafka.producer.KafkaTemplate;
import com.g7.framework.kafka.properties.KafkaProperties;
import com.g7.framework.kafka.util.ReadPropertiesUtils;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

import java.util.Properties;

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

    @Bean
    public <K, V> ProducerFactoryBean<K, V> producer() {

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

        return new ProducerFactoryBean<>(defaultProducerProperties);
    }

    @Bean
    @ConditionalOnMissingBean(value = KafkaTemplate.class)
    public <K, V> KafkaTemplate<K, V> kafkaTemplate(@Autowired Producer<K, V> producer) {
        return new KafkaTemplate<>(producer);
    }
}
