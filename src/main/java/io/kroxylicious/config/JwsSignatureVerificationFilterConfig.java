package io.kroxylicious.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Jackson configuration object for both the sample filters.<br />
 * Both filters perform the same transformation process (though on different types of messages and at
 * different points), only replacing one configured String value with another single configured String value,
 * meaning they can share a single configuration class.<br />
 * <br />
 * This configuration class accepts two String arguments: the value to be replaced, and the value it will be
 * replaced with.
 */
public class JwsSignatureVerificationFilterConfig {

    private final String jwks;

    public JwsSignatureVerificationFilterConfig(@JsonProperty(required = true) String jwks) {
        this.jwks = jwks;
    }

    public String getJwks() {
        return jwks;
    }
}
