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

@Slf4j
public class CachingSplitManager
        implements ConnectorSplitManager {

    private final ConnectorSplitManager delegate;

    public CachingSplitManager(
            ConnectorSplitManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {

        ConnectorSplitSource splitSource = delegate.getSplits(
                transaction,
                session,
                table,
                dynamicFilter,
                constraint);
        log.info("[SCENT SPI] Forwarded ConnectorSplitManager.getSplits(table)");
        return splitSource;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function) {

        ConnectorSplitSource splitSource = delegate.getSplits(
                transaction,
                session,
                function);
        log.info("[SCENT SPI] Forwarded ConnectorSplitManager.getSplits(function)");
        return splitSource;
    }
}