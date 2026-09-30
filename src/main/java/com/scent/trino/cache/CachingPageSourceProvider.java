package com.scent.trino.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public class CachingPageSourceProvider
        implements ConnectorPageSourceProvider {

    private final ConnectorPageSourceProvider delegate;

    public CachingPageSourceProvider(
            ConnectorPageSourceProvider delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {

        log.info("[SCENT SPI] Forwarded ConnectorPageSourceProvider.createPageSource");
        return delegate.createPageSource(
                transaction,
                session,
                split,
                table,
                tableCredentials,
                columns,
                dynamicFilter);
    }
}