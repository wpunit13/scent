package com.scent.trino.cache;

import com.scent.trino.BuildInfo;
import com.scent.trino.delegate.DelegateConnectorResolver;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class CachingConnectorFactory
        implements ConnectorFactory {

    private final DelegateConnectorResolver delegateResolver;

    public CachingConnectorFactory() {
        this.delegateResolver = new DelegateConnectorResolver();
    }

    @Override
    public String getName() {
        return "cache-wrapper";
    }

    @Override
    public Connector create(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context) {

        verifyTrinoVersion(catalogName, context);

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

    /**
     * The bundled delegate connectors and the plugin's own SPI usage are only valid on the Trino
     * version this plugin was built against. Fail fast with an actionable message instead of
     * surfacing a NoSuchMethodError/AbstractMethodError deep inside a query.
     */
    private static void verifyTrinoVersion(String catalogName, ConnectorContext context) {
        String runtimeVersion = context.getSpiVersion();
        if (!BuildInfo.trinoVersion().equals(runtimeVersion)) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    String.format(
                            "SCENT plugin in catalog '%s' was built for Trino %s but the cluster is running Trino %s. "
                                    + "Rebuild the plugin against the cluster's Trino version.",
                            catalogName, BuildInfo.trinoVersion(), runtimeVersion));
        }
    }
}