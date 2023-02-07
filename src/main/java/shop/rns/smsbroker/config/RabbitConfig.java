package shop.rns.smsbroker.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

import static shop.rns.smsbroker.utils.rabbitmq.RabbitUtil.*;

@Configuration
public class RabbitConfig {
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(jsonMessageConverter());
        return rabbitTemplate;
    }

    // SMS QUEUE
    @Bean
    Queue smsWorkKTQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", SENDER_EXCHANGE_NAME);
        args.put("x-dead-letter-routing-key", KT_SENDER_ROUTING_KEY);
        args.put("x-message-ttl", WORK_TTL);
        return new Queue(KT_WORK_QUEUE_NAME, true, false, false, args);
    }

    // SMS Exchange
    @Bean
    public DirectExchange smsExchange() {
        return new DirectExchange(SMS_EXCHANGE_NAME);
    }

    // SMS Binding
    @Bean
    public Binding bindingSmsKT(DirectExchange smsExchange, Queue smsWorkKTQueue) {
        return BindingBuilder.bind(smsWorkKTQueue)
                .to(smsExchange)
                .with(KT_WORK_ROUTING_KEY);
    }

    // Send Server에게 응답 결과 전달하기 위한 큐
    @Bean
    Queue smsReceiveKTQueue() {
        return new Queue(KT_RECEIVE_QUEUE_NAME, true);
    }

    @Bean
    public DirectExchange smsReceiveExchange() {
        return new DirectExchange(RECEIVE_EXCHANGE_NAME);
    }

    @Bean
    public Binding bindingSmsReceiveKT(DirectExchange smsReceiveExchange, Queue smsReceiveKTQueue) {
        return BindingBuilder.bind(smsReceiveKTQueue)
                .to(smsReceiveExchange)
                .with(KT_RECEIVE_ROUTING_KEY);
    }

    // DLX QUEUE
    @Bean
    public Queue smsWaitKTQueue(){
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", WAIT_TTL);
        args.put("x-dead-letter-exchange", SMS_EXCHANGE_NAME);
        args.put("x-dead-letter-routing-key", KT_WORK_ROUTING_KEY);
        return new Queue(KT_WAIT_QUEUE_NAME, true, false, false, args);
    }

    // DLX Exchange
    @Bean
    public DirectExchange dlxSMSExchange() {
        return new DirectExchange(WAIT_EXCHANGE_NAME);
    }


    // DLX SMS Binding
    @Bean
    public Binding bindingDLXSmsKT(DirectExchange dlxSMSExchange, Queue smsWaitKTQueue) {
        return BindingBuilder.bind(smsWaitKTQueue)
                .to(dlxSMSExchange)
                .with(KT_WAIT_ROUTING_KEY);
    }
}
