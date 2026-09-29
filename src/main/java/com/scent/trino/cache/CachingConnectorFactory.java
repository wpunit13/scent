package com.scent.trino.cache;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.plugin.bigquery.BigQueryPlugin;
import io.trino.plugin.oracle.OraclePlugin;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.spi.Plugin;

import java.util.HashMap;
import java.util.Map;

public class CachingConnectorFactory
        implements ConnectorFactory {

    private final Map<String, ConnectorFactory> delegateFactories;

    public CachingConnectorFactory() {
        this.delegateFactories = Map.of(
                "postgresql", getConnectorFactory(new PostgreSqlPlugin()),
                "oracle", getConnectorFactory(new OraclePlugin()),
                "bigquery", getConnectorFactory(new BigQueryPlugin()));
    }

    private static ConnectorFactory getConnectorFactory(Plugin plugin) {
        return plugin.getConnectorFactories()
                .iterator()
                .next();
    }

    @Override
    public String getName() {
        return "cached_wrapper";
    }

    @Override
    public Connector create(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context) {

        String delegateName = config.get("delegate.connector.name");
        if (delegateName == null || delegateName.isBlank()) {
            throw new IllegalArgumentException(
                "Missing required configuration: delegate.connector.name");
        }
        ConnectorFactory delegateFactory = delegateFactories.get(delegateName);
        if (delegateFactory == null) {
            throw new IllegalArgumentException(
                    "Unsupported delegate connector '" + delegateName
                            + "'. Supported connectors: " + delegateFactories.keySet());
        }

        Map<String, String> delegateConfig = new HashMap<>(config);
        delegateConfig.remove("delegate.connector.name");
        return CachingConnector.wrap(
            delegateFactory.create(catalogName, delegateConfig, context));
    }
}