package io.seg.framework.message.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;

import java.util.Map;

/**
 * @author dreamyao
 * @title 消息编码器
 * @date 2018/4/30 下午9:02
 * @since 2.0.0
 */
public class MessageEncoder<T> implements Serializer<T> {

    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("serializer.encoding");
        if (encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            if (data == null) {
                return null;
            } else {
                final MarshallerFactory marshallerFactory = Marshalling.getProvidedMarshallerFactory("serial");
                final MarshallingConfiguration configuration = new MarshallingConfiguration();
                configuration.setVersion(5);
                try (Marshaller marshaller = marshallerFactory.createMarshaller(configuration)) {

                    ByteBuf byteBuf = Unpooled.directBuffer();
                    BufferByteOutput output = new BufferByteOutput(byteBuf);

                    marshaller.start(output);
                    marshaller.writeObject(data);
                    marshaller.finish();

                    ByteBuf buffer = output.getBuffer();
                    return buffer.array();
                }
            }
        } catch (Exception e) {
            throw new SerializationException("Error when encoder java bean to byte[].");
        }
    }

    @Override
    public void close() {

    }
}
