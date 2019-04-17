package com.g7.framework.kafka;

import com.g7.framework.kafka.comsumer.EventCallback;
import org.springframework.stereotype.Component;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/22 上午10:54
 * @since 1.0.0
 */
@Component
public class EventConsumerExample implements EventCallback<Message> {

    @Override
    public String getTopic() {
        return "对应主题名称";
    }

    @Override
    public void consume(Message message) throws Exception {
        // 接受消息做业务处理
    }
}
