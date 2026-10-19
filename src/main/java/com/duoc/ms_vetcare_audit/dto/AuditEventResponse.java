package com.duoc.ms_vetcare_audit.dto;

import com.duoc.ms_vetcare_audit.entity.AuditEvent;

import java.time.LocalDateTime;

public class AuditEventResponse {

    private Long id;
    private String routingKey;
    private String payload;
    private LocalDateTime receivedAt;

    public AuditEventResponse() {
    }

    public AuditEventResponse(
            Long id,
            String routingKey,
            String payload,
            LocalDateTime receivedAt
    ) {
        this.id = id;
        this.routingKey = routingKey;
        this.payload = payload;
        this.receivedAt = receivedAt;
    }

    public static AuditEventResponse fromEntity(
            AuditEvent event
    ) {
        return new AuditEventResponse(
                event.getId(),
                event.getRoutingKey(),
                event.getPayload(),
                event.getReceivedAt()
        );
    }

    public Long getId() {
        return id;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public String getPayload() {
        return payload;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public void setReceivedAt(LocalDateTime receivedAt) {
        this.receivedAt = receivedAt;
    }
}