package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Offline {@link SecurityGate}: rather than introspecting each bearer token against security's
 * {@code GET /me}, the token's own EdDSA signature is verified against the public keys security
 * serves at {@code /.well-known/jwks.json}, and the user is read from the claims. One JWKS fetch
 * amortises over every request; the trade-off is revocation blindness until the token's {@code exp}.
 * Keys are cached; an unknown {@code kid} triggers one refetch, which also covers security
 * restarting with fresh ephemeral keys.
 *
 * <p>The verification core is the twin of the gates in microservice-comments/memes/formula
 * (audited 2026-07-07); it must stay in step with them.
 */
final class JwtSecurityGate implements SecurityGate {

    private static final String EXPECTED_ISSUER = "microservice-security";
    private static final byte[] ED25519_DER_PREFIX = HexFormat.of().parseHex("302a300506032b6570032100");

    private final Supplier<Map<String, PublicKey>> jwksFetcher;
    private final ObjectMapper mapper;
    private final AtomicReference<Map<String, PublicKey>> cachedKeys = new AtomicReference<>(Map.of());

    JwtSecurityGate(String securityUrl) {
        this.mapper = new ObjectMapper();
        this.jwksFetcher = jwksOver(securityUrl, mapper);
    }

    JwtSecurityGate(Supplier<Map<String, PublicKey>> jwksFetcher, ObjectMapper mapper) {
        this.jwksFetcher = jwksFetcher;
        this.mapper = mapper;
    }

    @Override
    public Optional<String> userFor(String accessToken) {
        try {
            String[] parts = accessToken.split("\\.");
            if (parts.length != 3) {
                return Optional.empty();
            }
            JsonNode header = json(parts[0]);
            if (!"EdDSA".equals(header.path("alg").asText())) {
                return Optional.empty();
            }
            PublicKey key = keyFor(header.path("kid").asText());
            if (key == null || !signatureVerifies(key, parts)) {
                return Optional.empty();
            }
            JsonNode claims = json(parts[1]);
            if (!EXPECTED_ISSUER.equals(claims.path("iss").asText())
                    || claims.path("exp").asLong() <= Instant.now().getEpochSecond()) {
                return Optional.empty();
            }
            String email = claims.path("sub").asText();
            return email.isBlank() ? Optional.empty() : Optional.of(email);
        } catch (Exception invalidTokenOrJwksDown) {
            return Optional.empty();
        }
    }

    private PublicKey keyFor(String kid) {
        PublicKey known = cachedKeys.get().get(kid);
        if (known != null) {
            return known;
        }
        // unknown kid: maybe a rotation (or security restarted with fresh keys) — refetch once
        Map<String, PublicKey> fresh = jwksFetcher.get();
        cachedKeys.set(fresh);
        return fresh.get(kid);
    }

    private static boolean signatureVerifies(PublicKey key, String[] parts) throws Exception {
        Signature verifier = Signature.getInstance("Ed25519");
        verifier.initVerify(key);
        verifier.update((parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII));
        return verifier.verify(Base64.getUrlDecoder().decode(parts[2]));
    }

    private JsonNode json(String base64Url) throws Exception {
        return mapper.readTree(Base64.getUrlDecoder().decode(base64Url));
    }

    private static Supplier<Map<String, PublicKey>> jwksOver(String securityUrl, ObjectMapper mapper) {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        return () -> {
            try {
                HttpResponse<String> response = client.send(
                        HttpRequest.newBuilder(URI.create(securityUrl + "/.well-known/jwks.json"))
                                .timeout(Duration.ofSeconds(5)).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                JsonNode set = mapper.readTree(response.body());
                Map<String, PublicKey> byKid = new HashMap<>();
                for (JsonNode jwk : set.path("keys")) {
                    if ("OKP".equals(jwk.path("kty").asText()) && "Ed25519".equals(jwk.path("crv").asText())) {
                        byKid.put(jwk.path("kid").asText(), publicKeyFrom(jwk.path("x").asText()));
                    }
                }
                return Map.copyOf(byKid);
            } catch (Exception jwksUnavailable) {
                return Map.of();   // no keys, no callers — fails closed
            }
        };
    }

    /** Rebuild the Ed25519 public key from a JWK's raw {@code x}: fixed DER prefix + 32 raw bytes. */
    private static PublicKey publicKeyFrom(String x) throws Exception {
        byte[] raw = Base64.getUrlDecoder().decode(x);
        byte[] encoded = new byte[ED25519_DER_PREFIX.length + raw.length];
        System.arraycopy(ED25519_DER_PREFIX, 0, encoded, 0, ED25519_DER_PREFIX.length);
        System.arraycopy(raw, 0, encoded, ED25519_DER_PREFIX.length, raw.length);
        return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));
    }
}
