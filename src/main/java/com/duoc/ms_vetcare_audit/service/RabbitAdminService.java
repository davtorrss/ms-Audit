package com.duoc.ms_vetcare_audit.service;

import com.duoc.ms_vetcare_audit.dto.RabbitBindingRequest;
import com.duoc.ms_vetcare_audit.dto.RabbitExchangeRequest;
import com.duoc.ms_vetcare_audit.dto.RabbitQueueRequest;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class RabbitAdminService {

    private final RabbitAdmin rabbitAdmin;

    public RabbitAdminService(RabbitAdmin rabbitAdmin) {
        this.rabbitAdmin = rabbitAdmin;
    }

    public Map<String, Object> createQueue(RabbitQueueRequest request) {

        Map<String, Object> arguments = new LinkedHashMap<>();

        if (request.getMessageTtl() != null) {
            arguments.put("x-message-ttl", request.getMessageTtl());
        }

        if (request.getDeadLetterExchange() != null
                && !request.getDeadLetterExchange().isBlank()) {
            arguments.put(
                    "x-dead-letter-exchange",
                    request.getDeadLetterExchange()
            );
        }

        if (request.getDeadLetterRoutingKey() != null
                && !request.getDeadLetterRoutingKey().isBlank()) {
            arguments.put(
                    "x-dead-letter-routing-key",
                    request.getDeadLetterRoutingKey()
            );
        }

        Queue queue = new Queue(
                request.getName(),
                request.isDurable(),
                false,
                false,
                arguments
        );

        String declaredQueue = rabbitAdmin.declareQueue(queue);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("message", "Cola creada correctamente");
        response.put("queue", declaredQueue != null ? declaredQueue : request.getName());
        response.put("durable", request.isDurable());
        response.put("arguments", arguments);

        return response;
    }

    public Map<String, Object> deleteQueue(String name) {

        boolean deleted = rabbitAdmin.deleteQueue(name);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put(
                "message",
                deleted
                        ? "Cola eliminada correctamente"
                        : "La cola no existía o no pudo eliminarse"
        );
        response.put("queue", name);
        response.put("deleted", deleted);

        return response;
    }

    public Map<String, Object> createExchange(RabbitExchangeRequest request) {

        Exchange exchange;

        switch (request.getType().toLowerCase()) {

            case "direct" ->
                    exchange = new DirectExchange(
                            request.getName(),
                            request.isDurable(),
                            false
                    );

            case "topic" ->
                    exchange = new TopicExchange(
                            request.getName(),
                            request.isDurable(),
                            false
                    );

            case "fanout" ->
                    exchange = new FanoutExchange(
                            request.getName(),
                            request.isDurable(),
                            false
                    );

            case "headers" ->
                    exchange = new HeadersExchange(
                            request.getName(),
                            request.isDurable(),
                            false
                    );

            default ->
                    throw new IllegalArgumentException(
                            "Tipo de exchange no soportado"
                    );
        }

        rabbitAdmin.declareExchange(exchange);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("message", "Exchange creado correctamente");
        response.put("exchange", request.getName());
        response.put("type", request.getType());
        response.put("durable", request.isDurable());

        return response;
    }

    public Map<String, Object> deleteExchange(String name) {

        boolean deleted = rabbitAdmin.deleteExchange(name);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put(
                "message",
                deleted
                        ? "Exchange eliminado correctamente"
                        : "El exchange no existía o no pudo eliminarse"
        );
        response.put("exchange", name);
        response.put("deleted", deleted);

        return response;
    }

    public Map<String, Object> createBinding(RabbitBindingRequest request) {

        Binding binding = new Binding(
                request.getQueue(),
                Binding.DestinationType.QUEUE,
                request.getExchange(),
                request.getRoutingKey(),
                Map.of()
        );

        rabbitAdmin.declareBinding(binding);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("message", "Binding creado correctamente");
        response.put("exchange", request.getExchange());
        response.put("queue", request.getQueue());
        response.put("routingKey", request.getRoutingKey());

        return response;
    }

    public Map<String, Object> deleteBinding(RabbitBindingRequest request) {

        Binding binding = new Binding(
                request.getQueue(),
                Binding.DestinationType.QUEUE,
                request.getExchange(),
                request.getRoutingKey(),
                Map.of()
        );

        rabbitAdmin.removeBinding(binding);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("message", "Binding eliminado correctamente");
        response.put("exchange", request.getExchange());
        response.put("queue", request.getQueue());
        response.put("routingKey", request.getRoutingKey());

        return response;
    }
}