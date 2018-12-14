package com.billionway.framework.kafka.factory;

/**
 * @author dreamyao
 * @title
 * @date 2018/5/9 下午9:41
 * @since 1.0.0
 */
public interface ExtensionFactory {

    /**
     * 从spring容器中获取Bean
     * @param type
     * @param name
     * @param <T>
     * @return
     */
    <T> T getExtension(Class<T> type, String name);
}
