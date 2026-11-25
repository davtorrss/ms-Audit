package com.duoc.ms_vetcare_audit.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaErrorConfig.class);

    /**
     * Configura el manejo de errores del consumidor Kafka de Audit.
     *
     * Si falla el procesamiento:
     * 1. Se reintenta hasta dos veces.
     * 2. Si continúa fallando, se publica en la DLT.
     * 3. Si la recuperación se completa, el consumidor puede
     *    continuar procesando otros registros.
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${vetcare.kafka.topics.audit-dlt}")
            String dltTopic
    ) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) ->
                                new TopicPartition(
                                        dltTopic,
                                        record.partition()
                                )
                );

        // Dos reintentos, con un segundo entre ellos.
        FixedBackOff backOff =
                new FixedBackOff(1000L, 2L);

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, backOff);

        errorHandler.setRetryListeners(
                (record, exception, deliveryAttempt) ->
                        log.warn(
                                "Fallo procesando Kafka. topic={}, partition={}, offset={}, intento={}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                deliveryAttempt,
                                exception
                        )
        );

        return errorHandler;
    }
}