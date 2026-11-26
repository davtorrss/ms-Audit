package com.duoc.ms_vetcare_audit.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class AuditTimelineKafkaPublisher {

    private static final Logger log =
            LoggerFactory.getLogger(AuditTimelineKafkaPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public AuditTimelineKafkaPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${vetcare.kafka.topics.timeline}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Publica un evento validado en el timeline de auditoria.
     *
     * Se utiliza eventId como clave para identificar cada
     * evento individualmente y preservar su historial.
     */
    public void publish(String eventId, String payload) {

        try {
            SendResult<String, String> result =
                    kafkaTemplate
                            .send(topic, eventId, payload)
                            .get(15, TimeUnit.SECONDS);

            log.info(
                    "Evento publicado en timeline. eventId={}, topic={}, partition={}, offset={}",
                    eventId,
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
            );

        } catch (InterruptedException error) {

            Thread.currentThread().interrupt();

            throw new IllegalStateException(
                    "Publicacion Kafka interrumpida. eventId=" + eventId,
                    error
            );

        } catch (ExecutionException | TimeoutException error) {

            throw new IllegalStateException(
                    "No se pudo confirmar la publicacion en audit.timeline. eventId="
                            + eventId,
                    error
            );
        }
    }
}