package com.duoc.ms_vetcare_audit.config;

import com.duoc.ms_vetcare_audit.messaging.RabbitNames;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    @Bean
    public RabbitAdmin rabbitAdmin(
            ConnectionFactory connectionFactory
    ) {

        return new RabbitAdmin(
                connectionFactory
        );
    }

    @Bean
    public DirectExchange directExchange() {

        return new DirectExchange(
                RabbitNames.DIRECT_EXCHANGE,
                true,
                false
        );
    }

    @Bean
    public TopicExchange topicExchange() {

        return new TopicExchange(
                RabbitNames.TOPIC_EXCHANGE,
                true,
                false
        );
    }

    @Bean
    public DirectExchange deadLetterExchange() {

        return new DirectExchange(
                RabbitNames.DLX_EXCHANGE,
                true,
                false
        );
    }

    @Bean
    public Queue auditQueue() {

        return QueueBuilder
                .durable(
                        RabbitNames.AUDIT_QUEUE
                )
                .deadLetterExchange(
                        RabbitNames.DLX_EXCHANGE
                )
                .deadLetterRoutingKey(
                        RabbitNames.AUDIT_DLQ_ROUTING_KEY
                )
                .ttl(60000)
                .build();
    }

    @Bean
    public Queue auditDeadLetterQueue() {

        return QueueBuilder
                .durable(
                        RabbitNames.AUDIT_DLQ
                )
                .build();
    }

    @Bean
    public Binding catalogAuditBinding(
            Queue auditQueue,
            TopicExchange topicExchange
    ) {

        return BindingBuilder
                .bind(auditQueue)
                .to(topicExchange)
                .with(
                        RabbitNames.CATALOG_EVENTS
                );
    }

    @Bean
    public Binding consultationAuditBinding(
            Queue auditQueue,
            TopicExchange topicExchange
    ) {

        return BindingBuilder
                .bind(auditQueue)
                .to(topicExchange)
                .with(
                        RabbitNames.CONSULTATION_EVENTS
                );
    }

    @Bean
    public Binding auditDeadLetterBinding(
            Queue auditDeadLetterQueue,
            DirectExchange deadLetterExchange
    ) {

        return BindingBuilder
                .bind(
                        auditDeadLetterQueue
                )
                .to(deadLetterExchange)
                .with(
                        RabbitNames.AUDIT_DLQ_ROUTING_KEY
                );
    }
}