package com.scent.trino.cache;

import io.trino.spi.connector.ConnectorMetadata;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

@Slf4j
public final class CachingMetadata implements InvocationHandler {

    private final ConnectorMetadata delegate;

    public CachingMetadata(ConnectorMetadata delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
    }

    public static ConnectorMetadata wrap(ConnectorMetadata delegate) {
        return (ConnectorMetadata) Proxy.newProxyInstance(
                ConnectorMetadata.class.getClassLoader(),
                new Class<?>[] {ConnectorMetadata.class},
                new CachingMetadata(delegate));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] arguments)
            throws Throwable {
        Object result;
        try {
            result = method.invoke(delegate, arguments);
        }
        catch (InvocationTargetException exception) {
            throw exception.getCause();
        }
        if (method.getDeclaringClass() != Object.class) {
            log.info("[SCENT SPI] Forwarded ConnectorMetadata.{}", method.getName());
        }
        return result;
    }
}