package io.seg.framework.message.comsumer;

/**
 * 消息监听抽象接口
 * @author dreamyao
 * @version V1.0
 * @date 2018/6/15 下午10:04
 */
public interface GenericMessageComsumer<T> extends Comsumer<T> {

    void onMessage(T data);

}
  