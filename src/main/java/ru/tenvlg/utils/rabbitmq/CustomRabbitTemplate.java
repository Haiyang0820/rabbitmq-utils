package ru.tenvlg.utils.rabbitmq;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

public class CustomRabbitTemplate extends RabbitTemplate {

    CustomRabbitTemplate(ConnectionFactory connectionFactory) {
        super(connectionFactory);
    }

    @Override
    protected Message doSendAndReceive(String exchange, String routingKey, Message message) {
        Message reply = super.doSendAndReceive(exchange, routingKey, message);
        if(reply == null) {
            throw new NoReplyException();
        }
        return reply;
    }
}
