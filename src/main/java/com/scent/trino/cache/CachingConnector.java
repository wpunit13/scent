package com.scent.trino.cache;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorMetadata;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

@Slf4j
public final class CachingConnector implements InvocationHandler {

    private final Connector delegate;

    public CachingConnector(Connector delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
    }

    public static Connector wrap(Connector delegate) {
        return (Connector) Proxy.newProxyInstance(
                Connector.class.getClassLoader(),
                new Class<?>[] {Connector.class},
                new CachingConnector(delegate));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] arguments)
            throws Throwable {
        try {
            Object result = method.invoke(delegate, arguments);
            if (method.getDeclaringClass() != Object.class) {
                log.info("[SCENT SPI] Forwarded Connector.{}", method.getName());
            }
            return wrapResult(method, result);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private Object wrapResult(Method method, Object result) {
        return switch (method.getName()) {
            case "getMetadata" -> CachingMetadata.wrap((ConnectorMetadata) result);
            case "getSplitManager" -> new CachingSplitManager((ConnectorSplitManager) result);
            case "getPageSourceProvider" -> new CachingPageSourceProvider((ConnectorPageSourceProvider) result);
            case "getPageSourceProviderFactory" -> {ConnectorPageSourceProviderFactory factory = (ConnectorPageSourceProviderFactory) result;
                yield (ConnectorPageSourceProviderFactory) () ->
                        new CachingPageSourceProvider(
                                factory.createPageSourceProvider());
            }

            default -> result;
        };
    }
}