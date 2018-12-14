package com.billionway.framework.kafka;

import java.io.Serializable;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/22 上午10:42
 * @since 1.0.0
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 6943882224787919258L;

    private String messageName;
    private String messageContext;

    public String getMessageName() {
        return messageName;
    }

    public void setMessageName(String messageName) {
        this.messageName = messageName;
    }

    public String getMessageContext() {
        return messageContext;
    }

    public void setMessageContext(String messageContext) {
        this.messageContext = messageContext;
    }
}
