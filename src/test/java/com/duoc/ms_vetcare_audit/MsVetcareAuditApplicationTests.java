package com.duoc.ms_vetcare_audit;

import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import org.springframework.security.oauth2.jwt.JwtDecoder;

@SpringBootTest
@ActiveProfiles("test")
class MsVetcareAuditApplicationTests {

    /*
     * Sustituye el decodificador JWT por un mock.
     * Evita contactar Microsoft Entra ID
     * durante esta prueba.
     */
    @MockitoBean
    private JwtDecoder jwtDecoder;

    /*
     * Verifica que Spring Boot puede iniciar
     * el contexto de Audit utilizando H2,
     * sin depender de PostgreSQL, RabbitMQ
     * ni Kafka externos.
     */
    @Test
    void contextLoads() {
    }
}
