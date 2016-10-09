package ru.tenvlg.utils.rabbitmq;

import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class ProtobufMessageListener implements IProtobufMessageListener
{
    private Map<Class<? extends Message>, Method> serviceMethods = null;

    private IProtobufService protobufService = null;

    public void setProtobufService(IProtobufService protobufService){
        this.protobufService = protobufService;

        Map<Class<? extends Message>, Method> map = new HashMap<>();
        for(Method m : protobufService.getClass().getMethods()){
            if(m.getParameterTypes().length!=1) continue;
            Class<?> parameterType = m.getParameterTypes()[0];
            if(Message.class.isAssignableFrom(parameterType) == false) continue;
            map.put((Class<? extends Message>)parameterType, m);
        }
        serviceMethods = Collections.unmodifiableMap(map);
    }

    @Override
    public Message onMessage(Message msg) {
        if(protobufService==null || serviceMethods==null){
            throw new RuntimeException("protobufService==null || serviceMethods==null");
        }
        Class<? extends Message> msgClass = msg.getClass();
        Method serviceMethod = serviceMethods.get(msgClass);
        if(serviceMethod==null){
            throw new RuntimeException("serviceMethods not contains " + msgClass.getName());
        }
        try{
            return (Message) serviceMethod.invoke(protobufService, msg);
        }catch (Exception e){
            throw new RuntimeException(String.format("protobufService=%s msg=%s", protobufService, msg), e);
        }

    }
}
