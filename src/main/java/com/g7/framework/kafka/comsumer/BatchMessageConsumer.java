package com.g7.framework.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * 批量消费
 * @author dreamyao
 * @version V1.0
 * @date 2018/6/15 下午9:53
 */
public interface BatchMessageConsumer<K, V> extends GenericMessageComsumer<List<ConsumerRecord<K, V>>> {

}
  