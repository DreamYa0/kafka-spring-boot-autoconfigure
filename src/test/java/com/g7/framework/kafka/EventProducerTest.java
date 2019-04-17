package com.g7.framework.kafka;

import com.g7.framework.kafka.producer.EventProducer;
import com.g7.framework.kafka.listener.ProducerListener;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Unit test for simple App.
 */
@Service
public class EventProducerTest {

    @Autowired
    private EventProducer eventProducer;

    public void sendMessage(){
        // 异步发送
        Message message = new Message();// 需要发送的消息，可以为任何对象
        eventProducer.asyncSend("主题名称", message, new ProducerListener<String, Object>() {
            // 异步发送回调监听器

            @Override
            public void onSuccess(String topic, Integer partition, String key, Object value, RecordMetadata recordMetadata) {

            }

            @Override
            public void onError(String topic, Integer partition, String key, Object value, Exception exception) {

            }

            @Override
            public boolean isInterestedInSuccess() {
                return false;
            }
        });

        // 同步发送
        eventProducer.syncSend("主题名称", message);
    }
}
