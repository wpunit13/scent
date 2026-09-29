package com.scent.trino.cache;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public final class CachingSplitManager
        implements ConnectorSplitManager {

    private final ConnectorSplitManager delegate;

    public CachingSplitManager(ConnectorSplitManager delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {

        log.info("[SCENT SPI] Intercepted ConnectorSplitManager.getSplits(table)");

        return delegate.getSplits(
                transaction,
                session,
                table,
                dynamicFilter,
                constraint);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function) {

        log.info("[SCENT SPI] Intercepted ConnectorSplitManager.getSplits(function)");

        return delegate.getSplits(
                transaction,
                session,
                function);
    }
}