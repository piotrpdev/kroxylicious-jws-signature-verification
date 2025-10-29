package io.kroxylicious;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.config.JwsSignatureVerificationFilterConfig;

/**
 * A {@link FilterFactory} for {@link JwsSignatureVerificationFilter}.
 */
@Plugin(configType = JwsSignatureVerificationFilterConfig.class)
public class JwsSignatureVerification implements FilterFactory<JwsSignatureVerificationFilterConfig, JwsSignatureVerificationFilterConfig> {

    @Override
    public JwsSignatureVerificationFilterConfig initialize(FilterFactoryContext context, JwsSignatureVerificationFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public JwsSignatureVerificationFilter createFilter(FilterFactoryContext context, JwsSignatureVerificationFilterConfig configuration) {
        return new JwsSignatureVerificationFilter(configuration);
    }

}
