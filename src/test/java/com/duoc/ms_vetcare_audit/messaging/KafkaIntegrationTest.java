
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
import org.springframework.kafka.listener.CommonErrorHandler;
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

    private static final String BROKER = "localhost:9092";

    private static final String SOURCE_TOPIC =
            "consultations.events";

    private static final String TIMELINE_TOPIC =
            "audit.timeline";

    private static final String DLT_TOPIC =
            "consultations.events.audit.DLT";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

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

            // Enviar el evento al tópico de consultas.
            kafkaTemplate.send(
                    SOURCE_TOPIC,
                    "999999",
                    payload
            ).get(15, TimeUnit.SECONDS);

            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(45);

            while (System.nanoTime() < deadline) {

                ConsumerRecords<String, String> records =
                        timelineConsumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    if (payload.equals(record.value())) {

                        assertEquals(
                                eventId,
                                record.key()
                        );

                        System.out.println(
                                "PRUEBA CORRECTA: evento recibido en audit.timeline"
                        );

                        System.out.println(
                                "EventId: " + eventId
                        );

                        System.out.println(
                                "Topico: " + record.topic()
                        );

                        System.out.println(
                                "Particion: " + record.partition()
                        );

                        System.out.println(
                                "Offset: " + record.offset()
                        );

                        return;
                    }
                }
            }

            fail("El evento válido no llegó a audit.timeline");
        }
    }

    @Test
    void mensajeIncorrectoTerminaEnDLT() throws Exception {

        String payload =
                "mensaje-invalido-" + UUID.randomUUID();

        try (KafkaConsumer<String, String> dltConsumer =
                     createVerificationConsumer("dlt")) {

            dltConsumer.subscribe(List.of(DLT_TOPIC));

            // Enviar un mensaje que no es JSON válido.
            kafkaTemplate.send(
                    SOURCE_TOPIC,
                    "999999",
                    payload
            ).get(15, TimeUnit.SECONDS);

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

                        System.out.println(
                                "Topico: " + record.topic()
                        );

                        System.out.println(
                                "Particion: " + record.partition()
                        );

                        System.out.println(
                                "Offset: " + record.offset()
                        );

                        return;
                    }
                }
            }

            fail("El mensaje incorrecto no llegó a la DLT");
        }
    }

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

        return new KafkaConsumer<>(properties);
    }

    /**
     * Contexto independiente de PostgreSQL y RabbitMQ.
     */
    @Configuration
    @EnableKafka
    @Import({
            ConsultationKafkaConsumer.class,
            AuditTimelineKafkaPublisher.class,
            KafkaErrorConfig.class
    })
    static class KafkaOnlyConfig {

        @Bean
        JsonMapper jsonMapper() {
            return JsonMapper.builder().build();
        }

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

            return new DefaultKafkaProducerFactory<>(properties);
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate(
                ProducerFactory<String, String> producerFactory
        ) {
            return new KafkaTemplate<>(producerFactory);
        }

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

            return new DefaultKafkaConsumerFactory<>(properties);
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String>
        kafkaListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory,
                CommonErrorHandler kafkaErrorHandler
        ) {

            ConcurrentKafkaListenerContainerFactory<String, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();

            factory.setConsumerFactory(consumerFactory);
            factory.setCommonErrorHandler(kafkaErrorHandler);

            return factory;
        }
    }
}
