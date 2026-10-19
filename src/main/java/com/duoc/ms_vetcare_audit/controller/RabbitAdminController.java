package com.duoc.ms_vetcare_audit.controller;

import com.duoc.ms_vetcare_audit.dto.RabbitBindingRequest;
import com.duoc.ms_vetcare_audit.dto.RabbitExchangeRequest;
import com.duoc.ms_vetcare_audit.dto.RabbitQueueRequest;
import com.duoc.ms_vetcare_audit.service.RabbitAdminService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/admin/rabbit")
public class RabbitAdminController {

    private final RabbitAdminService rabbitAdminService;

    public RabbitAdminController(RabbitAdminService rabbitAdminService) {
        this.rabbitAdminService = rabbitAdminService;
    }

    @PostMapping("/queues")
    public ResponseEntity<Map<String, Object>> createQueue(
            @Valid @RequestBody RabbitQueueRequest request) {

        return ResponseEntity.ok(
                rabbitAdminService.createQueue(request)
        );
    }

    @DeleteMapping("/queues/{name}")
    public ResponseEntity<Map<String, Object>> deleteQueue(
            @PathVariable String name) {

        return ResponseEntity.ok(
                rabbitAdminService.deleteQueue(name)
        );
    }

    @PostMapping("/exchanges")
    public ResponseEntity<Map<String, Object>> createExchange(
            @Valid @RequestBody RabbitExchangeRequest request) {

        return ResponseEntity.ok(
                rabbitAdminService.createExchange(request)
        );
    }

    @DeleteMapping("/exchanges/{name}")
    public ResponseEntity<Map<String, Object>> deleteExchange(
            @PathVariable String name) {

        return ResponseEntity.ok(
                rabbitAdminService.deleteExchange(name)
        );
    }

    @PostMapping("/bindings")
    public ResponseEntity<Map<String, Object>> createBinding(
            @Valid @RequestBody RabbitBindingRequest request) {

        return ResponseEntity.ok(
                rabbitAdminService.createBinding(request)
        );
    }

    @DeleteMapping("/bindings")
    public ResponseEntity<Map<String, Object>> deleteBinding(
            @Valid @RequestBody RabbitBindingRequest request) {

        return ResponseEntity.ok(
                rabbitAdminService.deleteBinding(request)
        );
    }
}