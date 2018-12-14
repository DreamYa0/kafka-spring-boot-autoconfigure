package com.billionway.framework.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public interface MessageComsumer<K, V> extends GenericMessageComsumer<ConsumerRecord<K, V>> {

}
  