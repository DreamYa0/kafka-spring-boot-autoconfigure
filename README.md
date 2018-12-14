# kafka-spring-boot-starter

#### 项目介绍
kafka-spring-boot-starter 是基于kafka client端进行封装的一个快捷、高效、简便的消息发布与订阅的客户端

#### 软件架构
软件架构说明
由于kafka producer天生线程安全，所以所有消息发送只需要公用一个producer

#### 使用说明

首先在SpringBoot配置文件中加入："kafka.bootstrap.servers":"xxxx",xxxx为kafka集群地址多个地址用逗号隔开

消息发送示例：

```java
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

```
或者

```java
@Service
public class EventProducerTest {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    public void sendMessage(){
        // 调用 KafkaTemplate 的方法进行消息发送
    }
}

```

消费者示例：

```java
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
        return "订阅的主题名称";
    }

    @Override
    public void consume(Message message) throws Exception {
        // 接受消息做业务处理
    }
}
```

#### 参与贡献

1. Fork 本项目
2. 新建 Feat_xxx 分支
3. 提交代码
4. 新建 Pull Request