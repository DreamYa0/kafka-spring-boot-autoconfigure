package io.seg.framework.message.comsumer;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午9:58
 * @since 1.0.0
 */
public interface EventCallback<T> {

    /**
     * @return Topic名称
     */
    String getTopic();

    /**
     * throw exception or return false will be treated as consumer fail.
     * @param message 从kafka发来的消息
     */
    void consume(T message) throws Exception;
}
