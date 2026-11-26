package com.duoc.ms_vetcare_audit.messaging;

import com.duoc.ms_vetcare_audit.config.KafkaErrorConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig(KafkaIntegrationTest.KafkaOnlyConfig.class)
@TestPropertySource(properties = {
        "spring.kafka.consumer.group-id=vetcare-audit-ev3-integration-test",
        "vetcare.kafka.topics.consultations=consultations.events",
        "vetcare.kafka.topics.timeline=audit.timeline",
        "vetcare.kafka.topics.audit-dlt=consultations.events.audit.DLT"
})
class KafkaIntegrationTest {

    private static final String BROKER =
        System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:19092,localhost:19093,localhost:19094"
        );

    private static final String SOURCE_TOPIC =
            "consultations.events";

    private static final String TIMELINE_TOPIC =
            "audit.timeline";

    private static final String DLT_TOPIC =
            "consultations.events.audit.DLT";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * PRUEBA 1:
     * Un evento valido debe procesarse y publicarse
     * en audit.timeline.
     */
    @Test
    void eventoValidoLlegaAlTimeline() throws Exception {

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

        try (KafkaConsumer<String, String> timelineConsumer =
                     createVerificationConsumer("timeline")) {

            timelineConsumer.subscribe(List.of(TIMELINE_TOPIC));

            // Publicar el mensaje de entrada en una transaccion.
            sendSourceEvent(payload);

            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(45);

            while (System.nanoTime() < deadline) {

                ConsumerRecords<String, String> records =
                        timelineConsumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    if (payload.equals(record.value())) {

                        assertEquals(eventId, record.key());

                        System.out.println(
                                "PRUEBA CORRECTA: evento recibido en audit.timeline"
                        );

                        System.out.println("EventId: " + eventId);
                        System.out.println("Topico: " + record.topic());
                        System.out.println("Particion: " + record.partition());
                        System.out.println("Offset: " + record.offset());

                        return;
                    }
                }
            }

            fail("El evento valido no llego a audit.timeline");
        }
    }

    /**
     * PRUEBA 2:
     * Un mensaje invalido debe provocar un rollback
     * y finalmente publicarse en la DLT.
     */
    @Test
    void mensajeIncorrectoTerminaEnDLT() throws Exception {

        String payload =
                "mensaje-invalido-" + UUID.randomUUID();

        try (KafkaConsumer<String, String> dltConsumer =
                     createVerificationConsumer("dlt")) {

            dltConsumer.subscribe(List.of(DLT_TOPIC));

            // Enviar un mensaje que no contiene JSON valido.
            sendSourceEvent(payload);

            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(45);

            while (System.nanoTime() < deadline) {

                ConsumerRecords<String, String> records =
                        dltConsumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    if (payload.equals(record.value())) {

                        System.out.println(
                                "PRUEBA CORRECTA: mensaje recibido en DLT"
                        );

                        System.out.println("Topico: " + record.topic());
                        System.out.println("Particion: " + record.partition());
                        System.out.println("Offset: " + record.offset());

                        return;
                    }
                }
            }

            fail("El mensaje incorrecto no llego a la DLT");
        }
    }

    /**
     * Publica eventos de prueba mediante una transaccion.
     *
     * El KafkaTemplate utilizado por estas pruebas tiene
     * un productor transaccional, por lo que el envio debe
     * ejecutarse dentro de una transaccion Kafka.
     */
    private void sendSourceEvent(String payload) {

        kafkaTemplate.executeInTransaction(operations -> {

            operations.send(
                    SOURCE_TOPIC,
                    "999999",
                    payload
            );

            return null;
        });
    }

    /**
     * Consumidor independiente para comprobar los resultados.
     *
     * read_committed impide que la prueba observe mensajes
     * de transacciones abortadas.
     */
    private KafkaConsumer<String, String> createVerificationConsumer(
            String purpose
    ) {

        Map<String, Object> properties = new HashMap<>();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BROKER
        );

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "vetcare-" + purpose + "-verification-"
                        + UUID.randomUUID()
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        properties.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                "read_committed"
        );

        return new KafkaConsumer<>(properties);
    }

    /**
     * Contexto de pruebas independiente de PostgreSQL
     * y RabbitMQ.
     *
     * Se configura explicitamente el productor transaccional,
     * el administrador de transacciones y el procesador
     * de rollback.
     */
    @Configuration
    @EnableKafka
    @Import({
            ConsultationKafkaConsumer.class,
            AuditTimelineKafkaPublisher.class,
            KafkaErrorConfig.class
    })
    static class KafkaOnlyConfig {

        private static final String TRANSACTION_PREFIX =
                "vetcare-audit-integration-"
                        + UUID.randomUUID() + "-";

        @Bean
        JsonMapper jsonMapper() {
            return JsonMapper.builder().build();
        }

        /**
         * Productor Kafka transaccional.
         */
        @Bean
        ProducerFactory<String, String> producerFactory() {

            Map<String, Object> properties = new HashMap<>();

            properties.put(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    BROKER
            );

            properties.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class
            );

            properties.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class
            );

            properties.put(
                    ProducerConfig.ACKS_CONFIG,
                    "all"
            );

            properties.put(
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                    true
            );

            DefaultKafkaProducerFactory<String, String> factory =
                    new DefaultKafkaProducerFactory<>(properties);

            factory.setTransactionIdPrefix(
                    TRANSACTION_PREFIX
            );

            return factory;
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate(
                ProducerFactory<String, String> producerFactory
        ) {

            return new KafkaTemplate<>(producerFactory);
        }

        /**
         * Administrador de transacciones Kafka.
         *
         * Utiliza el mismo ProducerFactory del KafkaTemplate.
         */
        @Bean
        KafkaTransactionManager<String, String> kafkaTransactionManager(
                ProducerFactory<String, String> producerFactory
        ) {

            return new KafkaTransactionManager<>(producerFactory);
        }

        /**
         * Consumidor de Audit.
         */
        @Bean
        ConsumerFactory<String, String> consumerFactory() {

            Map<String, Object> properties = new HashMap<>();

            properties.put(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    BROKER
            );

            properties.put(
                    ConsumerConfig.GROUP_ID_CONFIG,
                    "vetcare-audit-ev3-integration-test"
            );

            properties.put(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class
            );

            properties.put(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class
            );

            properties.put(
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    "earliest"
            );

            properties.put(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                    false
            );

            properties.put(
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                    "read_committed"
            );

            return new DefaultKafkaConsumerFactory<>(properties);
        }

        /**
         * Contenedor Kafka configurado para transacciones.
         */
        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String>
        kafkaListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory,
                DefaultAfterRollbackProcessor<String, String>
                        kafkaAfterRollbackProcessor,
                KafkaTransactionManager<String, String>
                        kafkaTransactionManager
        ) {

            ConcurrentKafkaListenerContainerFactory<String, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();

            factory.setConsumerFactory(consumerFactory);

            // Reemplaza el antiguo CommonErrorHandler.
            factory.setAfterRollbackProcessor(
                    kafkaAfterRollbackProcessor
            );

            // Coordina las transacciones del consumidor.
            factory.getContainerProperties()
                    .setKafkaAwareTransactionManager(
                            kafkaTransactionManager
                    );

            factory.getContainerProperties().setAckMode(
                    ContainerProperties.AckMode.RECORD
            );

            return factory;
        }
    }
}
