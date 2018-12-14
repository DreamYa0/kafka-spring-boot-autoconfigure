package com.billionway.framework.kafka.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.jboss.marshalling.ByteInput;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.Unmarshaller;

import java.util.Map;

/**
 * @author dreamyao
 * @title 消息解码器
 * @date 2018/4/30 下午9:20
 * @since 2.0.0
 */
public class MessageDecoder<T> implements Deserializer<T> {

    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("deserializer.encoding");
        if (encodingValue != null && encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            } else {
                final MarshallerFactory marshallerFactory = Marshalling.getProvidedMarshallerFactory("serial");
                final MarshallingConfiguration configuration = new MarshallingConfiguration();
                configuration.setVersion(5);
                try (Unmarshaller unmarshaller = marshallerFactory.createUnmarshaller(configuration)) {

                    ByteBuf byteBuf = Unpooled.directBuffer(data.length);
                    if (byteBuf.isWritable()) {
                        byteBuf.writeBytes(data, 0, data.length - 1);
                    }

                    ByteInput input = new BufferByteInput(byteBuf);

                    unmarshaller.start(input);
                    Object object = unmarshaller.readObject();
                    unmarshaller.finish();

                    return (T) object;
                }
            }
        } catch (Exception e) {
            throw new SerializationException("Error when decoder byte[] to java bean.");
        }
    }

    @Override
    public void close() {

    }
}
