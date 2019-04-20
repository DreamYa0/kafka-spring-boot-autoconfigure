package com.g7.framework.kafka.schema;

import com.g7.framework.kafka.properties.KafkaProperties;
import com.g7.framework.kafka.comsumer.EventConsumer;
import com.g7.framework.kafka.container.KafkaConsumerFactory;
import com.g7.framework.kafka.factory.ProducerFactoryBean;
import com.g7.framework.kafka.producer.EventProducer;
import com.g7.framework.kafka.producer.KafkaTemplate;
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
    public ProducerFactoryBean<String, Object> producer() {
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
    public KafkaTemplate<String, Object> kafkaTemplate(@Autowired Producer<String, Object> producer) {
        return new KafkaTemplate<>(producer,false);
    }

    @Bean
    @ConditionalOnMissingBean(value = EventConsumer.class)
    public EventConsumer eventConsumer() {
        Properties consumerDefaultProperties = ReadPropertiesUtils.readConsumerDefaultProperties();

        consumerDefaultProperties.setProperty("bootstrap.servers", properties.getBootstrap().getServers());

        getConsumerDeserializer(consumerDefaultProperties);

        return new EventConsumer(consumerDefaultProperties);
    }

    @Bean
    @ConditionalOnMissingBean(value = EventProducer.class)
    public EventProducer eventProducer(@Autowired Producer<String, Object> producer) {
        return new EventProducer(producer);
    }

    @Bean
    @ConditionalOnMissingBean(value = KafkaConsumerFactory.class)
    public KafkaConsumerFactory kafkaConsumerFactory() {
        Properties consumerDefaultProperties = ReadPropertiesUtils.readConsumerDefaultProperties();

        consumerDefaultProperties.setProperty("bootstrap.servers", properties.getBootstrap().getServers());

        getConsumerDeserializer(consumerDefaultProperties);

        return new KafkaConsumerFactory(consumerDefaultProperties);
    }

    private void getConsumerDeserializer(Properties consumerDefaultProperties) {

        String keyDeserializer = properties.getConsumer().getKeyDeserializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(keyDeserializer))) {
            consumerDefaultProperties.setProperty("key.deserializer", keyDeserializer);
        }

        String valueDeserializer = properties.getConsumer().getValueDeserializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(valueDeserializer))) {
            consumerDefaultProperties.setProperty("value.deserializer", valueDeserializer);
        }
    }
}
