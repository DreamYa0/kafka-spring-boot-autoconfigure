package com.ntocc.framework.kafka.factory;

import org.springframework.context.ApplicationContext;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author dreamyao
 * @title
 * @date 2018/5/9 下午9:42
 * @since 1.0.0
 */
public class SpringExtensionFactory implements ExtensionFactory {

    private static final Set<ApplicationContext> contexts = new CopyOnWriteArraySet<>();

    public static void addApplicationContext(ApplicationContext context) {
        contexts.add(context);
    }

    public static void removeApplicationContext(ApplicationContext context) {
        contexts.remove(context);
    }

    @Override
    public <T> T getExtension(Class<T> type, String name) {
        for (ApplicationContext context : contexts) {
            if (context.containsBean(name)) {
                return context.getBean(name, type);
            }
        }
        return null;
    }
}
