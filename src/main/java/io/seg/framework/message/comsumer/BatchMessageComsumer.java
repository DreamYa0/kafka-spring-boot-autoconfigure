package io.seg.framework.message.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * 批量消费
 * @author dreamyao
 * @version V1.0
 * @date 2018/6/15 下午9:53
 */
public interface BatchMessageComsumer<K, V> extends GenericMessageComsumer<List<ConsumerRecord<K, V>>> {

}
  