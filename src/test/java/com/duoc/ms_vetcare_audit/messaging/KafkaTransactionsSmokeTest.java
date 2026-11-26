package com.duoc.ms_vetcare_audit.messaging;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class KafkaTransactionsSmokeTest {

    @Test
    void brokerPermiteIniciarYAbortarUnaTransaccion() {

        Map<String, Object> properties = new HashMap<>();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        // Identificador distinto para cada ejecución.
        properties.put(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                "vetcare-audit-tx-test-" + UUID.randomUUID()
        );

        try (
                KafkaProducer<String, String> producer =
                        new KafkaProducer<>(properties)
        ) {

            // Conectar con el coordinador de transacciones.
            producer.initTransactions();

            // Iniciar una transacción de prueba.
            producer.beginTransaction();

            // Finalizarla sin publicar mensajes.
            producer.abortTransaction();

            System.out.println(
                    "PRUEBA CORRECTA: Kafka permite iniciar y abortar transacciones"
            );
        }
    }
}
