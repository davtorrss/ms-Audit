
package com.duoc.ms_vetcare_audit.messaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig({
        KafkaIntegrationTest.KafkaOnlyConfig.class,
        KafkaRollbackReprocessingTest.FaultConfig.class
})
@TestPropertySource(properties = {
        "spring.kafka.consumer.group-id=vetcare-audit-ev3-integration-test",
        "vetcare.kafka.topics.consultations=consultations.events",
        "vetcare.kafka.topics.timeline=audit.timeline",
        "vetcare.kafka.topics.audit-dlt=consultations.events.audit.DLT"
})
class KafkaRollbackReprocessingTest {

    private static final String BROKER = "localhost:9092";
    private static final String SOURCE = "consultations.events";
    private static final String TIMELINE = "audit.timeline";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private FaultOncePublisher publisher;

    @Test
    void rollbackNoProduceUnDuplicadoVisible() {

        String eventId = UUID.randomUUID().toString();

        String payload = """
                {
                  "eventId": "%s",
                  "consultationId": 999999,
                  "serviceId": 1,
                  "eventType": "CONSULTATION_CREATED",
                  "previousStatus": null,
                  "status": "SOLICITADA",
                  "occurredAt": "2026-11-25T18:00:00Z"
                }
                """.formatted(eventId);

        // Preparar un fallo solamente para este evento.
        publisher.failOnceFor(eventId);

        // Enviar el evento de entrada correctamente.
        kafkaTemplate.executeInTransaction(operations -> {
            operations.send(SOURCE, "999999", payload);
            return null;
        });

        int visibleCount = 0;
        boolean finishedReading = false;

        Map<String, Object> config = new HashMap<>();

        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BROKER
        );
        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "vetcare-rollback-verification-" + UUID.randomUUID()
        );
        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );
        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );
        config.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                "read_committed"
        );
        config.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        try (KafkaConsumer<String, String> observer =
                     new KafkaConsumer<>(config)) {

            // Observar todas las particiones del timeline.
            List<TopicPartition> partitions =
                    observer.partitionsFor(TIMELINE)
                            .stream()
                            .map(info -> new TopicPartition(
                                    info.topic(),
                                    info.partition()
                            ))
                            .toList();

            observer.assign(partitions);
            observer.seekToBeginning(partitions);

            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(45);

            while (System.nanoTime() < deadline) {

                var records = observer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    if (eventId.equals(record.key())
                            && payload.equals(record.value())) {
                        visibleCount++;
                    }
                }

                // Una vez ocurrido el fallo y su reprocesamiento,
                // comprobar que alcanzamos el final visible.
                if (publisher.getAttempts() >= 2 && visibleCount >= 1) {

                    Map<TopicPartition, Long> endOffsets =
                            observer.endOffsets(partitions);

                    finishedReading = true;

                    for (TopicPartition partition : partitions) {
                        if (observer.position(partition)
                                < endOffsets.get(partition)) {
                            finishedReading = false;
                            break;
                        }
                    }

                    if (finishedReading) {
                        break;
                    }
                }
            }
        }

        assertEquals(
                2,
                publisher.getAttempts(),
                "El evento debio procesarse dos veces"
        );

        assertTrue(
                finishedReading,
                "No se alcanzo a verificar el timeline completo"
        );

        assertEquals(
                1,
                visibleCount,
                "Solo debe existir una publicacion confirmada visible"
        );

        System.out.println(
                "PRUEBA CORRECTA: hubo un rollback y un reprocesamiento"
        );
        System.out.println(
                "Intentos de publicacion: " + publisher.getAttempts()
        );
        System.out.println(
                "Eventos visibles en read_committed: " + visibleCount
        );
    }

    /**
     * Configuracion exclusiva de esta prueba.
     * No altera el publicador de produccion.
     */
    @Configuration
    static class FaultConfig {

        @Bean
        @Primary
        FaultOncePublisher faultOncePublisher(
                KafkaTemplate<String, String> kafkaTemplate
        ) {
            return new FaultOncePublisher(
                    kafkaTemplate,
                    TIMELINE
            );
        }
    }

    /**
     * Publicador de prueba que provoca una excepcion
     * despues del primer envio, pero antes de confirmar
     * la transaccion del consumidor.
     */
    static class FaultOncePublisher
            extends AuditTimelineKafkaPublisher {

        private final AtomicReference<String> failingEventId =
                new AtomicReference<>();

        private final AtomicInteger attempts =
                new AtomicInteger();

        FaultOncePublisher(
                KafkaTemplate<String, String> kafkaTemplate,
                String topic
        ) {
            super(kafkaTemplate, topic);
        }

        void failOnceFor(String eventId) {
            attempts.set(0);
            failingEventId.set(eventId);
        }

        int getAttempts() {
            return attempts.get();
        }

        @Override
        public void publish(String eventId, String payload) {

            // Enviar realmente el mensaje mediante Kafka.
            super.publish(eventId, payload);

            if (eventId.equals(failingEventId.get())
                    && attempts.incrementAndGet() == 1) {

                throw new IllegalStateException(
                        "Fallo simulado despues del envio Kafka"
                );
            }
        }
    }
}
