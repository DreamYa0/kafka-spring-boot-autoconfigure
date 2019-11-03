package com.g7.framework.kafka.comsumer;

import java.util.HashMap;

/**
 * @author dreamyao
 * @title 
 * @date 2019/11/3 12:13 AM
 * @since 1.0.0
 */
public enum SubscribeTypeEnum {

    /**
     * 每个线程维护一个KafkaConsumer,每个KafkaConsumer串行处理records消息
     * 优点：方便实现速度较快，因为不需要任何线程间交互易于维护分区内的消息顺序
     * 缺点：更多的TCP连接开销(每个线程都要维护若干个TCP连接)consumer数受限于topic分区数，扩展性差频繁请求导致吞吐量下降，线程自己处理消费到的消息可能会导致超时，从而造成rebalance
     */
    MANY_CONSUMER_ONE_WORKER(0, "MANY_CONSUMER_ONE_WORKER"),
    /**
     * 单个(或多个)consumer，多个worker线程
     * 优点：可独立扩展consumer数和worker数，伸缩性好
     * 缺点：实现麻烦通常难于维护分区内的消息顺序处理链路变长，导致难以保证提交位移的语义正确性
     */
    ONE_CONSUMER_MANY_WORKER(1, "ONE_CONSUMER_MANY_WORKER"),

    /**
     * 每个线程维护一个KafkaConsumer,每个KafkaConsumer并行处理records消息
     */
    MANY_CONSUMER_MANY_WORKER(2,"MANY_CONSUMER_MANY_WORKER"),
    ;

    private Integer code;
    private String name;

    SubscribeTypeEnum(Integer code, String name) {
        this.code = code;
        this.name = name;
    }

    public Integer getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    private final static HashMap<Integer, SubscribeTypeEnum> VALUE_MAP = new HashMap<>();

    static {
        for (SubscribeTypeEnum o : SubscribeTypeEnum.values()) {
            VALUE_MAP.put(o.getCode(), o);
        }
    }

    public static SubscribeTypeEnum valueOf(Integer siteStatusCode) {
        if (siteStatusCode == null) {
            return null;
        }
        SubscribeTypeEnum v = VALUE_MAP.get(siteStatusCode);
        if (v == null) {
            throw new RuntimeException("Unkonw Code: " + siteStatusCode);
        }
        return v;
    }

    public static Boolean contain(String name) {
        if (name == null) {
            return false;
        }
        for (SubscribeTypeEnum typeEnum : SubscribeTypeEnum.values()) {
            if (typeEnum.name().equals(name)) {
                return true;
            }
        }
        return false;
    }

    public static Boolean contain(Integer code) {
        return VALUE_MAP.containsKey(code);
    }
}
