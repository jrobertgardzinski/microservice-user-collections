package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.identity.UserId;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
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
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The offline gate against real Ed25519 tokens: it must accept a well-signed, unexpired token from
 * security and reject everything else — tampering, expiry, a foreign issuer, a wrong key.
 */
@Epic("Infrastructure")
@Feature("Offline token verification")
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
        Caller caller = gate.callerFor(token).orElseThrow();
        assertEquals("alice@example.com", caller.email());
        assertEquals(Optional.empty(), caller.userId(), "an address as subject carries no id");
    }

    @Test
    void reads_the_id_from_the_subject_and_the_address_from_its_claim() throws Exception {
        UUID id = UUID.randomUUID();
        String token = token("k1", "microservice-security", id.toString(), "alice@example.com",
                Instant.now().plusSeconds(3600), keys);
        Caller caller = gate.callerFor(token).orElseThrow();
        assertEquals("alice@example.com", caller.email());
        assertEquals(Optional.of(new UserId(id)), caller.userId());
    }

    @Test
    void rejects_a_tampered_signature() throws Exception {
        String token = token("k1", "microservice-security", "alice@example.com",
                Instant.now().plusSeconds(3600), keys);
        // Tamper by flipping a bit in the signature's FIRST byte (the R point — always
        // significant), not by swapping the trailing base64 chars: an Ed25519 signature's last
        // byte is the top byte of the scalar S < 2^252, so it lands in 0..15 — about 1 run in 16
        // it is exactly 4 ("BA"), and the old suffix swap to "BB" only touched the 4 trailing
        // base64 bits that the lenient URL decoder discards, leaving the signature semantically
        // intact and the test red.
        int dot = token.lastIndexOf('.');
        byte[] sig = Base64.getUrlDecoder().decode(token.substring(dot + 1));
        sig[0] ^= 0x01;
        String tampered = token.substring(0, dot + 1) + B64.encodeToString(sig);
        assertTrue(gate.callerFor(tampered).isEmpty());
    }

    @Test
    void rejects_an_expired_token() throws Exception {
        String token = token("k1", "microservice-security", "alice@example.com",
                Instant.now().minusSeconds(60), keys);
        assertTrue(gate.callerFor(token).isEmpty());
    }

    @Test
    void rejects_a_foreign_issuer() throws Exception {
        String token = token("k1", "someone-else", "alice@example.com",
                Instant.now().plusSeconds(3600), keys);
        assertTrue(gate.callerFor(token).isEmpty());
    }

    @Test
    void rejects_a_token_signed_by_an_unknown_key() throws Exception {
        KeyPair stranger = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        String token = token("k1", "microservice-security", "alice@example.com",
                Instant.now().plusSeconds(3600), stranger);
        assertTrue(gate.callerFor(token).isEmpty());
    }

    private static String token(String kid, String issuer, String subject, Instant expiry, KeyPair signer)
            throws Exception {
        return token(kid, issuer, subject, null, expiry, signer);
    }

    private static String token(String kid, String issuer, String subject, String email, Instant expiry,
                                KeyPair signer) throws Exception {
        String header = B64.encodeToString(("{\"alg\":\"EdDSA\",\"kid\":\"" + kid + "\"}")
                .getBytes(StandardCharsets.UTF_8));
        String emailClaim = email == null ? "" : ",\"email\":\"" + email + "\"";
        String claims = B64.encodeToString(("{\"iss\":\"" + issuer + "\",\"sub\":\"" + subject
                + "\"" + emailClaim + ",\"exp\":" + expiry.getEpochSecond() + "}").getBytes(StandardCharsets.UTF_8));
        Signature signature = Signature.getInstance("Ed25519");
        signature.initSign(signer.getPrivate());
        signature.update((header + "." + claims).getBytes(StandardCharsets.US_ASCII));
        // ensure it really is an Ed25519 key we signed with (guards the test's own setup)
        assert signer.getPublic() instanceof EdECPublicKey;
        return header + "." + claims + "." + B64.encodeToString(signature.sign());
    }
}
