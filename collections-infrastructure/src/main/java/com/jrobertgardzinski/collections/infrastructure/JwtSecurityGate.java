package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.offlinejwt.OfflineJwtVerifier;
import com.jrobertgardzinski.offlinejwt.VerifiedToken;

import java.security.PublicKey;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Offline {@link SecurityGate}: rather than introspecting each bearer token against security's
 * {@code GET /me}, the token's own EdDSA signature is verified against the public keys security
 * serves at {@code /.well-known/jwks.json}, and the user is read from the claims. One JWKS fetch
 * amortises over every request; the trade-off is revocation blindness until the token's
 * {@code exp}. The verification core is the shared offline-jwt library (it used to be a local
 * copy with four twins); this service reads only the subject — roles play no part here.
 */
final class JwtSecurityGate implements SecurityGate {

    private final OfflineJwtVerifier verifier;

    JwtSecurityGate(String securityUrl) {
        this.verifier = OfflineJwtVerifier.overHttp(securityUrl, new ObjectMapper());
    }

    JwtSecurityGate(Supplier<Map<String, PublicKey>> jwksFetcher, ObjectMapper mapper) {
        this.verifier = new OfflineJwtVerifier(jwksFetcher, mapper);
    }

    @Override
    public Optional<String> userFor(String accessToken) {
        return verifier.verify(accessToken).map(VerifiedToken::subject);
    }
}
