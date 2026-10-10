package com.scent.trino.delegate;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

/**
 * Resolves a delegate {@link ConnectorFactory} by connector name.
 *
 * <p> The delegate plugin is loaded reflectively so TRACE has no compile-time dependency on the
 * connector artifacts (which Trino no longer publishes to Maven Central from 477 onward). The
 * delegate plugin must be present on the plugin classpath at runtime.
 */
public class DelegateConnectorResolver {

    private static final Map<String, String> DELEGATE_PLUGINS = Map.of(
            "postgresql", "io.trino.plugin.postgresql.PostgreSqlPlugin",
            "oracle", "io.trino.plugin.oracle.OraclePlugin",
            "bigquery", "io.trino.plugin.bigquery.BigQueryPlugin");

    public ConnectorFactory resolve(String name) {
        String pluginClassName = DELEGATE_PLUGINS.get(name);
        if (pluginClassName == null) {
            throw new IllegalArgumentException("Unsupported delegate connector: " + name);
        }

        return getConnectorFactory(instantiatePlugin(pluginClassName), name);
    }

    private static Plugin instantiatePlugin(String className) {
        try {
            return (Plugin) Class.forName(className).getDeclaredConstructor().newInstance();
        }
        catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Delegate plugin not found on the classpath: " + className, e);
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate delegate plugin: " + className, e);
        }
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
