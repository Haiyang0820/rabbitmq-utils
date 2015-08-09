package ru.tenvlg.utils.rabbitmq;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;

import java.util.Map;

@SuppressWarnings("unused")
public abstract class ProtobufMessageConverter extends AbstractMessageConverter {

    private static final String MESSAGE_TYPE_NAME = "message_type_name";
    private static final String CONTENT_TYPE_PROTOBUF = "application/x-google-protobuf";

    private final Descriptors.FileDescriptor descriptor;
    private final Map<Descriptors.Descriptor,Parser<?>> parsers;

    protected ProtobufMessageConverter(Descriptors.FileDescriptor descriptor, Map<Descriptors.Descriptor, Parser<?>> parsers){
        this.descriptor = descriptor;
        this.parsers = parsers;
    }

    private String getMessageTypeName(Message msg){
        Map<String, Object> headers = msg.getMessageProperties().getHeaders();
        return headers.get(MESSAGE_TYPE_NAME).toString();
    }

    @Override
    protected Message createMessage(Object object, MessageProperties messageProperties) {
        if(!(object instanceof com.google.protobuf.Message)){
            throw new MessageConversionException("Message wasn't a protobuf");
        } else {
            com.google.protobuf.Message protobuf = (com.google.protobuf.Message)object;
            byte[] byteArray = protobuf.toByteArray();
            messageProperties.setContentLength(byteArray.length);
            messageProperties.setContentType(CONTENT_TYPE_PROTOBUF);
            messageProperties.setHeader(
                    MESSAGE_TYPE_NAME,
                    protobuf.getDescriptorForType().getName()
            );
            return new Message(byteArray, messageProperties);
        }
    }

    @Override
    public Object fromMessage(Message message) throws MessageConversionException {
        String name = getMessageTypeName(message);
        Parser<?> parser = parsers.get(descriptor.findMessageTypeByName(name));
        if(parser == null){
            throw new AmqpRejectAndDontRequeueException(String.format("Cannot convert, unknown parser for message type %s", name));
        }
        Object object;
        try {
            object = parser.parseFrom(message.getBody());
        } catch (InvalidProtocolBufferException e) {
            throw new AmqpRejectAndDontRequeueException(e.getMessage(), e);
        }
        if(object==null){
            throw new AmqpRejectAndDontRequeueException(String.format("Cannot convert, unknown message type %s", name));
        }
        return object;
    }
}
