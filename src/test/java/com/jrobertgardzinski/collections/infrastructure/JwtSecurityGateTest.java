package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.Signature;
import java.security.interfaces.EdECPublicKey;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The offline gate against real Ed25519 tokens: it must accept a well-signed, unexpired token from
 * security and reject everything else — tampering, expiry, a foreign issuer, a wrong key.
 */
class JwtSecurityGateTest {

    private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();

    private KeyPair keys;
    private JwtSecurityGate gate;

    @BeforeEach
    void freshKey() throws Exception {
        keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        Map<String, PublicKey> jwks = Map.of("k1", keys.getPublic());
        gate = new JwtSecurityGate(() -> jwks, new ObjectMapper());
    }

    @Test
    void accepts_a_valid_token_and_reads_the_user() throws Exception {
        String token = token("k1", "microservice-security", "alice@example.com",
                Instant.now().plusSeconds(3600), keys);
        assertEquals("alice@example.com", gate.userFor(token).orElseThrow());
    }

    @Test
    void rejects_a_tampered_signature() throws Exception {
        String token = token("k1", "microservice-security", "alice@example.com",
                Instant.now().plusSeconds(3600), keys);
        String tampered = token.substring(0, token.length() - 2) + (token.endsWith("A") ? "BB" : "AA");
        assertTrue(gate.userFor(tampered).isEmpty());
    }

    @Test
    void rejects_an_expired_token() throws Exception {
        String token = token("k1", "microservice-security", "alice@example.com",
                Instant.now().minusSeconds(60), keys);
        assertTrue(gate.userFor(token).isEmpty());
    }

    @Test
    void rejects_a_foreign_issuer() throws Exception {
        String token = token("k1", "someone-else", "alice@example.com",
                Instant.now().plusSeconds(3600), keys);
        assertTrue(gate.userFor(token).isEmpty());
    }

    @Test
    void rejects_a_token_signed_by_an_unknown_key() throws Exception {
        KeyPair stranger = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        String token = token("k1", "microservice-security", "alice@example.com",
                Instant.now().plusSeconds(3600), stranger);
        assertTrue(gate.userFor(token).isEmpty());
    }

    private static String token(String kid, String issuer, String subject, Instant expiry, KeyPair signer)
            throws Exception {
        String header = B64.encodeToString(("{\"alg\":\"EdDSA\",\"kid\":\"" + kid + "\"}")
                .getBytes(StandardCharsets.UTF_8));
        String claims = B64.encodeToString(("{\"iss\":\"" + issuer + "\",\"sub\":\"" + subject
                + "\",\"exp\":" + expiry.getEpochSecond() + "}").getBytes(StandardCharsets.UTF_8));
        Signature signature = Signature.getInstance("Ed25519");
        signature.initSign(signer.getPrivate());
        signature.update((header + "." + claims).getBytes(StandardCharsets.US_ASCII));
        // ensure it really is an Ed25519 key we signed with (guards the test's own setup)
        assert signer.getPublic() instanceof EdECPublicKey;
        return header + "." + claims + "." + B64.encodeToString(signature.sign());
    }
}
