package ru.tenvlg.utils.rabbitmq;

import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unused")
public interface IProtobufMessageListener {
    Message onMessage(Message msg);
}
