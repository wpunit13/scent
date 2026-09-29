package com.scent.trino.cache;

import com.scent.trino.delegate.DelegateConnectorResolver;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.HashMap;
import java.util.Map;

public class CachingConnectorFactory
        implements ConnectorFactory {

    private final DelegateConnectorResolver delegateResolver;

    public CachingConnectorFactory() {
        this.delegateResolver = new DelegateConnectorResolver();
    }

    @Override
    public String getName() {
        return "caching_wrapper";
    }

    @Override
    public Connector create(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context) {

        String delegateName = config.get("delegate.connector.name");
        if (delegateName == null || delegateName.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration: delegate.connector.name");
        }

        ConnectorFactory delegateFactory = delegateResolver.resolve(delegateName);

        Map<String, String> delegateConfig = new HashMap<>(config);
        delegateConfig.remove("delegate.connector.name");

        Connector delegateConnector =
                delegateFactory.create(
                        catalogName,
                        delegateConfig,
                        context);

        return CachingConnector.wrap(delegateConnector);
    }
}