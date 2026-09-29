package com.scent.trino.delegate;

import io.trino.plugin.bigquery.BigQueryPlugin;
import io.trino.plugin.oracle.OraclePlugin;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

public class DelegateConnectorResolver {

    private final Map<String, ConnectorFactory> factories;

    public DelegateConnectorResolver() {
        this.factories = Map.of("postgresql", getConnectorFactory(new PostgreSqlPlugin(), "postgresql"),
                "oracle", getConnectorFactory(new OraclePlugin(), "oracle"),
                "bigquery", getConnectorFactory(new BigQueryPlugin(), "bigquery")
        );
    }

    public ConnectorFactory resolve(String name) {
        ConnectorFactory factory = factories.get(name);

        if (factory == null) {
            throw new IllegalArgumentException("Unsupported delegate connector: " + name);
        }

        return factory;
    }

    private static ConnectorFactory getConnectorFactory(Plugin plugin, String connectorName) {

        for (ConnectorFactory factory : plugin.getConnectorFactories()) {
            if (factory.getName().equals(connectorName)) {
                return factory;
            }
        }

        throw new IllegalArgumentException("Connector factory not found: " + connectorName);
    }
}
