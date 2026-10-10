package com.scent.trino.cache;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

@Slf4j
public final class CachingConnector implements InvocationHandler {

    private final Connector delegate;
    private final String partitionColumn;
    private final InMemorySliceStore sliceStore;

    public CachingConnector(Connector delegate, String partitionColumn, InMemorySliceStore sliceStore) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.partitionColumn = Objects.requireNonNull(partitionColumn, "partitionColumn is null");
        this.sliceStore = Objects.requireNonNull(sliceStore, "sliceStore is null");
    }

    public static Connector wrap(Connector delegate, String partitionColumn, InMemorySliceStore sliceStore) {
        return (Connector) Proxy.newProxyInstance(
                Connector.class.getClassLoader(),
                new Class<?>[] {Connector.class},
                new CachingConnector(delegate, partitionColumn, sliceStore));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
        try {
            Object result = method.invoke(delegate, arguments);
            if (method.getDeclaringClass() != Object.class) {
                log.debug("[SCENT SPI] Forwarded Connector.{}", method.getName());
            }
            return wrapResult(method, result);
        }
        catch (InvocationTargetException e) {
            throw e.getCause() != null ? e.getCause() : e;
        }
    }

    private Object wrapResult(Method method, Object result) {
        return switch (method.getName()) {
            case "getMetadata" -> CachingMetadata.wrap((ConnectorMetadata) result, partitionColumn, sliceStore.isParquetEnabled());
            case "getSplitManager" -> new CachingSplitManager((ConnectorSplitManager) result);
            case "getPageSourceProvider" -> new CachingPageSourceProvider((ConnectorPageSourceProvider) result, sliceStore);
            case "getPageSourceProviderFactory" -> {ConnectorPageSourceProviderFactory factory = (ConnectorPageSourceProviderFactory) result;
                yield (ConnectorPageSourceProviderFactory) () ->
                        new CachingPageSourceProvider(
                                factory.createPageSourceProvider(),
                                sliceStore);
            }

            default -> result;
        };
    }
}