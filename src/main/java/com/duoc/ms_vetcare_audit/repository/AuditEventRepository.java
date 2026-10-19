package com.duoc.ms_vetcare_audit.repository;

import com.duoc.ms_vetcare_audit.entity.AuditEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AuditEventRepository
        extends JpaRepository<AuditEvent, Long> {
}