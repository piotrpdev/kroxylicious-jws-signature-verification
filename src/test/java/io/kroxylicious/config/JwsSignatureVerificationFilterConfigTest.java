package io.kroxylicious.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JwsSignatureVerificationFilterConfigTest {

    private static final String CONFIG_JWKS =
        "{\"keys\":[{" +
            "\"kty\":\"EC\"," +
            "\"use\":\"sig\"," +
            "\"kid\":\"the key\"," +
            "\"x\":\"amuk6RkDZi-48mKrzgBN_zUZ_9qupIwTZHJjM03qL-4\"," +
            "\"y\":\"ZOESj6_dpPiZZR-fJ-XVszQta28Cjgti7JudooQJ0co\"," +
            "\"crv\":\"P-256\"" +
        "}]}";

    @Test
    void validateJwks() {
        JwsSignatureVerificationFilterConfig config = new JwsSignatureVerificationFilterConfig(CONFIG_JWKS);
        assertThat(config.getJwks()).isEqualTo(CONFIG_JWKS);
    }
}
