package io.kroxylicious;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.config.JwsSignatureVerificationFilterConfig;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwk.VerificationJwkSelector;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.lang.JoseException;

/**
 * A sample ProduceRequestFilter implementation, intended to demonstrate how custom filters work with
 * Kroxylicious.<br />
 * <br />
 * This filter transforms the partition data sent by a Kafka producer in a produce request by replacing all
 * occurrences of the String "foo" with the String "bar". These strings are configurable in the config file,
 * so you could substitute this with any text you want.<br />
 * <br />
 * An example of a use case where this might be applicable is when producers are sending data to Kafka
 * using different formats from what consumers are expecting. You could configure this filter to transform
 * the data sent by producers to Kafka into the format consumers expect. In this example use case, the filter
 * could be further modified to apply different transformations to different topics, or when sent by
 * particular producers.
 */
class JwsSignatureVerificationFilter implements ProduceRequestFilter {

    private final JwsSignatureVerificationFilterConfig config;

    JwsSignatureVerificationFilter(JwsSignatureVerificationFilterConfig config) {
        this.config = config;
    }

    /**
     * Handle the given request, transforming the data in-place according to the configuration, and returning
     * the ProduceRequestData instance to be passed to the next filter.
     *
     * @param apiVersion the apiVersion of the request
     * @param header     request header.
     * @param request    The KRPC message to handle.
     * @param context    The context.
     * @return CompletionStage that will yield a {@link RequestFilterResult} containing the request to be forwarded.
     */
    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        verifyUsingJwks(request, context);
        return context.forwardRequest(header, request);
    }

    /**
     * Applies the transformation to the request data.
     * @param request the request to be transformed
     * @param context the context
     */
    private void verifyUsingJwks(ProduceRequestData request, FilterContext context) {
        for (ProduceRequestData.TopicProduceData topicData : request.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionProduceData : topicData.partitionData()) {
                for (Record record : ((Records) partitionProduceData.records()).records()) {
                    String recordString = new String(StandardCharsets.UTF_8.decode(record.value()).array());

                    try {
                        verifyStringUsingJwksFromConfig(recordString);
                    } catch (JoseException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private void verifyStringUsingJwksFromConfig(String recordString) throws JoseException {
        JsonWebSignature jws = new JsonWebSignature();
        jws.setAlgorithmConstraints(new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT, AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256));

        jws.setCompactSerialization(recordString);

        // Create a new JsonWebKeySet object with the JWK Set JSON
        JsonWebKeySet jsonWebKeySet = new JsonWebKeySet(config.getJwks());

        // The JWS header contains information indicating which key was used to secure the JWS.
        // In this case (as will hopefully often be the case) the JWS Key ID
        // corresponds directly to the Key ID in the JWK Set.
        // The VerificationJwkSelector looks at Key ID, Key Type, designated use (signatures vs. encryption),
        // and the designated algorithm in order to select the appropriate key for verification from
        // a set of JWKs.
        VerificationJwkSelector jwkSelector = new VerificationJwkSelector();
        JsonWebKey jwk = jwkSelector.select(jws, jsonWebKeySet.getJsonWebKeys());

        // The verification key on the JWS is the public key from the JWK we pulled from the JWK Set.
        jws.setKey(jwk.getKey());

        // Check the signature
        boolean signatureVerified = jws.verifySignature();

        // Do something useful with the result of signature verification
        System.out.println("JWS Signature is valid: " + signatureVerified);

        // Get the payload, or signed content, from the JWS
        String payload = jws.getPayload();

        // Do something useful with the content
        System.out.println("JWS payload: " + payload);
    }

}
