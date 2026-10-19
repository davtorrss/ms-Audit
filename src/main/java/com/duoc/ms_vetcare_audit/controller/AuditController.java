package com.duoc.ms_vetcare_audit.controller;

import com.duoc.ms_vetcare_audit.dto.AuditEventResponse;
import com.duoc.ms_vetcare_audit.service.AuditService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/audit/events")
public class AuditController {

    private final AuditService auditService;

    public AuditController(AuditService auditService) {
        this.auditService = auditService;
    }

    @GetMapping
    public List<AuditEventResponse> findAll() {
        return auditService.findAll();
    }

    @GetMapping("/{id}")
    public AuditEventResponse findById(@PathVariable Long id) {
        return auditService.findById(id);
    }
}