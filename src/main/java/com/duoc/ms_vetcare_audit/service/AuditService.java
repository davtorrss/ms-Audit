package com.duoc.ms_vetcare_audit.service;

import com.duoc.ms_vetcare_audit.dto.AuditEventResponse;
import com.duoc.ms_vetcare_audit.entity.AuditEvent;
import com.duoc.ms_vetcare_audit.repository.AuditEventRepository;

import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class AuditService {

    private final AuditEventRepository repository;

    public AuditService(
            AuditEventRepository repository
    ) {
        this.repository = repository;
    }

    public AuditEventResponse saveEvent(
            String routingKey,
            String payload
    ) {

        AuditEvent event =
                new AuditEvent(
                        routingKey,
                        payload,
                        LocalDateTime.now()
                );

        AuditEvent saved =
                repository.save(event);

        return AuditEventResponse.fromEntity(saved);
    }

    public List<AuditEventResponse> findAll() {

        return repository.findAll()
                .stream()
                .map(AuditEventResponse::fromEntity)
                .toList();
    }

    public AuditEventResponse findById(
            Long id
    ) {

        AuditEvent event =
                repository.findById(id)
                        .orElseThrow(() ->
                                new RuntimeException(
                                        "Evento de auditoría no encontrado con id: " + id
                                )
                        );

        return AuditEventResponse.fromEntity(event);
    }
}