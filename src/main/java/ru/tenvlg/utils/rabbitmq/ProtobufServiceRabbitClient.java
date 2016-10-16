package ru.tenvlg.utils.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.DisposableBean;

@SuppressWarnings("unused")
public abstract class ProtobufServiceRabbitClient implements DisposableBean {
    private final static Logger log = LoggerFactory.getLogger(ProtobufServiceRabbitClient.class);

    protected final ConnectionFactory connectionFactory;
    protected final RabbitTemplate rabbitTemplate;
    protected final RabbitAdmin rabbitAdmin;

    private SimpleMessageListenerContainer replyListenerContainer;

    public ProtobufServiceRabbitClient(ConnectionFactory connectionFactory, ProtobufMessageConverter messageConverter){
        this.connectionFactory = connectionFactory;

        this.rabbitTemplate = new RabbitTemplate(connectionFactory);
        this.rabbitTemplate.setMessageConverter(messageConverter);

        this.rabbitAdmin = new RabbitAdmin(connectionFactory);
    }

    public void setExchange(String exchange){
        rabbitTemplate.setExchange(exchange);
    }

    public void setRoutingKey(String routingKey){
        rabbitTemplate.setRoutingKey(routingKey);
    }

    public void setReplyTimeout(long replyTimeout){
        rabbitTemplate.setReplyTimeout(replyTimeout);
    }

    public void setReplyQueue(Queue replyQueue){
        if(replyListenerContainer!=null) throw new RuntimeException();

        rabbitAdmin.declareQueue(replyQueue);
        rabbitTemplate.setReplyQueue(replyQueue);

        replyListenerContainer = new SimpleMessageListenerContainer(connectionFactory);
        replyListenerContainer.setQueues(replyQueue);
        replyListenerContainer.setMessageListener(rabbitTemplate);
        replyListenerContainer.setErrorHandler(t -> log.error("replyListenerContainer threw error", t));
        replyListenerContainer.start();
    }

    @Override
    public void destroy() throws Exception {
        if(replyListenerContainer!=null){
            String[] queueNames = replyListenerContainer.getQueueNames();
            replyListenerContainer.destroy();
            for(String queueName : queueNames){
                rabbitAdmin.deleteQueue(queueName);
            }
        }
    }

}
