package com.scent.trino.delegate;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Set;

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
            "oracle", "io.trino.plugin.oracle.OraclePlugin");

    // Multi-split delegates are unsafe with the current one-slice-per-partition cache key: every
    // delegate split would return the full slice on a hit (duplicate rows) and clobber the file on
    // a miss. Gate until slices are keyed per delegate split (ADR-012).
    private static final Set<String> MULTI_SPLIT_DELEGATES = Set.of("bigquery");

    public ConnectorFactory resolve(String name) {
        if (MULTI_SPLIT_DELEGATES.contains(name)) {
            throw new IllegalArgumentException(
                    "Delegate connector '" + name + "' is not supported yet: it emits multiple splits per scan, "
                            + "which SCENT's one-slice-per-partition cache key cannot cache safely (duplicate rows).");
        }
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
