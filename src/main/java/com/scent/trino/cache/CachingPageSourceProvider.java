package com.scent.trino.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class CachingPageSourceProvider
        implements ConnectorPageSourceProvider {

    private final ConnectorPageSourceProvider delegate;

    public CachingPageSourceProvider(
            ConnectorPageSourceProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {

        ConnectorPageSource pageSource = delegate.createPageSource(
                transaction,
                session,
                split,
                table,
                columns,
                dynamicFilter);
        log.info("[SCENT SPI] Forwarded ConnectorPageSourceProvider.createPageSource");
        return pageSource;
    }
}