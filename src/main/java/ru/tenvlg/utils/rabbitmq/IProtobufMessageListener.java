package ru.tenvlg.utils.rabbitmq;

import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by Vladislav on 14.10.2015.
 */
@SuppressWarnings("unused")
public interface IProtobufMessageListener {
    Message onMessage(Message msg) throws InvocationTargetException, IllegalAccessException;
}
