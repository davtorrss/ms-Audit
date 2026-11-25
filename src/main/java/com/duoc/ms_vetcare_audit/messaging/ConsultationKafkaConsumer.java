
package com.duoc.ms_vetcare_audit.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.format.DateTimeParseException;

@Component
public class ConsultationKafkaConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(ConsultationKafkaConsumer.class);

    private final JsonMapper jsonMapper;
    private final AuditTimelineKafkaPublisher timelinePublisher;

    public ConsultationKafkaConsumer(
            JsonMapper jsonMapper,
            AuditTimelineKafkaPublisher timelinePublisher
    ) {
        this.jsonMapper = jsonMapper;
        this.timelinePublisher = timelinePublisher;
    }

    @KafkaListener(
            topics = "${vetcare.kafka.topics.consultations}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, String> record) {

        String payload = record.value();

        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException(
                    "El evento Kafka recibido está vacío"
            );
        }

        // Convertir el mensaje a una estructura JSON.
        JsonNode event;

        try {
            event = jsonMapper.readTree(payload);
        } catch (Exception error) {
            throw new IllegalArgumentException(
                    "El mensaje recibido no contiene JSON válido",
                    error
            );
        }

        if (event == null || !event.isObject()) {
            throw new IllegalArgumentException(
                    "El evento debe ser un objeto JSON"
            );
        }

        // Validar los campos obligatorios.
        String eventId = requiredText(event, "eventId");
        String eventType = requiredText(event, "eventType");
        String status = requiredText(event, "status");
        String occurredAt = requiredText(event, "occurredAt");

        JsonNode idNode = event.get("consultationId");

        if (idNode == null
                || !idNode.isIntegralNumber()
                || idNode.asLong() <= 0) {

            throw new IllegalArgumentException(
                    "consultationId debe ser un número positivo"
            );
        }

        long consultationId = idNode.asLong();

        // Verificar que la clave corresponde a la consulta.
        if (!String.valueOf(consultationId).equals(record.key())) {
            throw new IllegalArgumentException(
                    "La clave Kafka no coincide con consultationId"
            );
        }

        // Aceptar únicamente los eventos conocidos.
        if (!eventType.equals("CONSULTATION_CREATED")
                && !eventType.equals("CONSULTATION_STATUS_CHANGED")) {

            throw new IllegalArgumentException(
                    "Tipo de evento desconocido: " + eventType
            );
        }

        // Validar la fecha del evento.
        try {
            Instant.parse(occurredAt);
        } catch (DateTimeParseException error) {
            throw new IllegalArgumentException(
                    "occurredAt tiene un formato de fecha inválido",
                    error
            );
        }

        log.info(
                "Evento Kafka validado. eventId={}, consultationId={}, type={}, status={}, partition={}, offset={}",
                eventId,
                consultationId,
                eventType,
                status,
                record.partition(),
                record.offset()
        );

        /*
         * Publicar el evento válido en audit.timeline.
         *
         * Si la publicación falla, la excepción llega al
         * manejador KafkaErrorConfig, que aplicará reintentos
         * y recuperación mediante la DLT.
         */
        timelinePublisher.publish(eventId, payload);

        log.info(
                "Evento procesado y enviado al timeline. eventId={}",
                eventId
        );
    }

    private String requiredText(JsonNode event, String field) {

        JsonNode value = event.get(field);

        if (value == null
                || !value.isTextual()
                || value.asText().isBlank()) {

            throw new IllegalArgumentException(
                    "Campo obligatorio inválido: " + field
            );
        }

        return value.asText();
    }
}
