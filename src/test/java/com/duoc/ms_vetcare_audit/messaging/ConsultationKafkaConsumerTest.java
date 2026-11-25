
package com.duoc.ms_vetcare_audit.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConsultationKafkaConsumerTest {

    private final AuditTimelineKafkaPublisher publisher =
            mock(AuditTimelineKafkaPublisher.class);

    private final ConsultationKafkaConsumer consumer =
            new ConsultationKafkaConsumer(
                    JsonMapper.builder().build(),
                    publisher
            );

    @Test
    void aceptaEventoValidoYLoPublicaEnTimeline() {

        String payload = """
                {
                  "eventId": "test-001",
                  "consultationId": 999999,
                  "serviceId": 1,
                  "eventType": "CONSULTATION_CREATED",
                  "previousStatus": null,
                  "status": "SOLICITADA",
                  "occurredAt": "2026-11-25T18:00:00Z"
                }
                """;

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>(
                        "consultations.events",
                        0,
                        0L,
                        "999999",
                        payload
                );

        assertDoesNotThrow(() -> consumer.consume(record));

        verify(publisher, times(1)).publish(
                "test-001",
                payload
        );
    }

    @Test
    void rechazaMensajeMalFormadoSinPublicarlo() {

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>(
                        "consultations.events",
                        0,
                        1L,
                        "999999",
                        "hola, no soy un evento JSON"
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> consumer.consume(record)
        );

        verifyNoInteractions(publisher);
    }
}
