package io.seg.framework.message.schema;

import io.seg.framework.message.comsumer.EventConsumer;
import io.seg.framework.message.factory.ProducerFactoryBean;
import io.seg.framework.message.producer.EventProducer;
import io.seg.framework.message.producer.KafkaTemplate;
import io.seg.framework.message.util.ReadPropertiesUtils;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
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
@Configurable
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
    public KafkaTemplate<String, Object> kafkaTemplate(@Autowired Producer<String, Object> producer) {
        return new KafkaTemplate<>(producer,false);
    }

    @Bean
    public EventConsumer eventConsumer() {
        Properties consumerDefaultProperties = ReadPropertiesUtils.readConsumerDefaultProperties();
        consumerDefaultProperties.setProperty("bootstrap.servers", servers);
        return new EventConsumer(consumerDefaultProperties);
    }

    @Bean
    public EventProducer eventProducer(@Autowired Producer<String, Object> producer) {
        return new EventProducer(producer);
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }
}
