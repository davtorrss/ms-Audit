package com.duoc.ms_vetcare_audit.messaging;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Component
public class AuditEventConsumer {

    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 1000;

    @RabbitListener(queues = RabbitNames.AUDIT_QUEUE)
    public void consume(
            Message message,
            Channel channel
    ) throws IOException {

        long deliveryTag =
                message.getMessageProperties().getDeliveryTag();

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {

            try {

                processMessage(message);

                channel.basicAck(
                        deliveryTag,
                        false
                );

                System.out.println(
                        "ACK enviado. Evento de Auditoría procesado correctamente."
                );

                return;

            } catch (Exception e) {

                System.err.println(
                        "Error procesando evento de Auditoría. Intento "
                                + attempt
                                + "/"
                                + MAX_RETRIES
                                + " - "
                                + e.getMessage()
                );

                if (attempt < MAX_RETRIES) {

                    long backoff =
                            INITIAL_BACKOFF_MS
                                    * (1L << (attempt - 1));

                    System.out.println(
                            "Reintentando en "
                                    + backoff
                                    + " ms..."
                    );

                    try {

                        Thread.sleep(backoff);

                    } catch (InterruptedException interruptedException) {

                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        System.err.println(
                "Se agotaron los reintentos. Enviando evento de Auditoría a DLQ."
        );

        channel.basicNack(
                deliveryTag,
                false,
                false
        );
    }

    private void processMessage(
            Message message
    ) {

        String body =
                new String(
                        message.getBody(),
                        StandardCharsets.UTF_8
                );

        if (body.isBlank()) {

            throw new IllegalArgumentException(
                    "El evento recibido está vacío"
            );
        }

        String receivedRoutingKey =
                message
                        .getMessageProperties()
                        .getReceivedRoutingKey();

        System.out.println(
                "Evento recibido en Auditoría:"
        );

        System.out.println(
                "Routing key: " + receivedRoutingKey
        );

        System.out.println(
                "Payload: " + body
        );
    }
}