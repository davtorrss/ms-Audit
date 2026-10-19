package com.duoc.ms_vetcare_audit.messaging;

public final class RabbitNames {

    private RabbitNames() {
    }

    // Exchanges compartidos
    public static final String DIRECT_EXCHANGE =
            "vetcare.direct.exchange";

    public static final String TOPIC_EXCHANGE =
            "vetcare.topic.exchange";

    public static final String DLX_EXCHANGE =
            "vetcare.dlx.exchange";

    // Cola de auditoría
    public static final String AUDIT_QUEUE =
            "vetcare.audit.queue";

    public static final String AUDIT_DLQ =
            "vetcare.audit.dlq";

    // Routing key hacia DLQ
    public static final String AUDIT_DLQ_ROUTING_KEY =
            "audit.dlq";

    // Bindings que auditoría escuchará
    public static final String CATALOG_EVENTS =
            "catalog.#";

    public static final String CONSULTATION_EVENTS =
            "consultation.#";
}