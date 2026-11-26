
package com.duoc.ms_vetcare_audit.config;

import java.time.Duration;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;

import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaErrorConfig.class);

    /**
     * Recuperacion transaccional del consumidor Kafka.
     *
     * Si el procesamiento falla:
     * 1. La transaccion actual se revierte.
     * 2. El evento puede reprocesarse hasta dos veces.
     * 3. Si sigue fallando, se envia a la DLT.
     * 4. La publicacion en DLT y la confirmacion del
     *    offset recuperado se coordinan en otra transaccion.
     */
    @Bean
    public DefaultAfterRollbackProcessor<String, String>
            kafkaAfterRollbackProcessor(
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

        // Si la DLT no confirma el envio, no dar
        // la recuperacion por completada.
        recoverer.setFailIfSendResultIsError(true);

        recoverer.setWaitForSendResultTimeout(
                Duration.ofSeconds(15)
        );

        // Dos reintentos, separados por un segundo.
        FixedBackOff backOff =
                new FixedBackOff(1000L, 2L);

        DefaultAfterRollbackProcessor<String, String> processor =
                new DefaultAfterRollbackProcessor<>(
                        recoverer,
                        backOff,
                        kafkaTemplate,
                        true
                );

        processor.setRetryListeners(
                (record, exception, deliveryAttempt) ->
                        log.warn(
                                "Reprocesamiento Kafka tras rollback. " +
                                "topic={}, partition={}, offset={}, intento={}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                deliveryAttempt,
                                exception
                        )
        );

        return processor;
    }
}
