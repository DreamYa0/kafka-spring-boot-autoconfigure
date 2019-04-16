package com.ntocc.framework.kafka.schema;

import com.ntocc.framework.kafka.comsumer.EventConsumer;
import com.ntocc.framework.kafka.container.KafkaConsumerFactory;
import com.ntocc.framework.kafka.factory.ProducerFactoryBean;
import com.ntocc.framework.kafka.producer.EventProducer;
import com.ntocc.framework.kafka.producer.KafkaTemplate;
import com.ntocc.framework.kafka.util.ReadPropertiesUtils;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

/**
 * @author dreamyao
 * @title
 * @date 2018/8/5 上午11:39
 * @since 1.0.0
 */
@ConditionalOnProperty(value = "kafka.bootstrap.servers")
@ConfigurationProperties(prefix = "kafka.bootstrap")
public class EventMessageConfiguration {

    private String servers;

    @Bean
    public ProducerFactoryBean<String, Object> producer() {
        Properties defaultProducerProperties = ReadPropertiesUtils.readProducerDefaultProperties();
        defaultProducerProperties.setProperty("bootstrap.servers", servers);
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
        consumerDefaultProperties.setProperty("bootstrap.servers", servers);
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
        consumerDefaultProperties.setProperty("bootstrap.servers", servers);
        return new KafkaConsumerFactory(consumerDefaultProperties);
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }
}
