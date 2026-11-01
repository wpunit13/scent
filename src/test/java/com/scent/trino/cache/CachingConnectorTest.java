package com.scent.trino.cache;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingConnectorTest {

    private static final class StubMetadata implements ConnectorMetadata {
        final ConnectorTableHandle handle = new ConnectorTableHandle() {};
        RuntimeException tableHandleFailure;

        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session,
                SchemaTableName tableName,
                Optional<ConnectorTableVersion> startVersion,
                Optional<ConnectorTableVersion> endVersion) {
            if (tableHandleFailure != null) {
                throw tableHandleFailure;
            }
            return handle;
        }
    }

    private static final class StubConnector implements Connector {
        final StubMetadata metadata = new StubMetadata();
        final ConnectorSplitManager splitManager = (transaction, session, table, columns, constraint) -> null;
        final ConnectorPageSourceProviderFactory pageSourceProviderFactory = () -> new ConnectorPageSourceProvider() {};
        boolean shutdown;
        RuntimeException failure;

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            return splitManager;
        }

        @Override
        public ConnectorPageSourceProviderFactory getPageSourceProviderFactory() {
            return pageSourceProviderFactory;
        }

        @Override
        public void shutdown() {
            shutdown = true;
            if (failure != null) {
                throw failure;
            }
        }
    }

    @Test
    void forwardsShutdownToDelegate() {
        StubConnector stub = new StubConnector();
        Connector connector = CachingConnector.wrap(stub, Map.of("s.t", List.of("part")), new FileSliceStore(1024 * 1024, null, null, Map.of()), new TableMetadataCache(), new PlanningNotes(null, null), Map.of());

        connector.shutdown();

        assertTrue(stub.shutdown);
    }

    @Test
    void wrapsSpiSubObjectsAndForwardsCalls() {
        StubConnector stub = new StubConnector();
        Connector connector = CachingConnector.wrap(stub, Map.of("s.t", List.of("part")), new FileSliceStore(1024 * 1024, null, null, Map.of()), new TableMetadataCache(), new PlanningNotes(null, null), Map.of());

        assertInstanceOf(OutageSafeSplitManager.class, connector.getSplitManager());
        ConnectorPageSourceProviderFactory wrappedFactory = (ConnectorPageSourceProviderFactory) connector.getPageSourceProviderFactory();
        assertInstanceOf(CachingPageSourceProvider.class, wrappedFactory.createPageSourceProvider());

        ConnectorMetadata metadata = connector.getMetadata(null, null);
        assertInstanceOf(CachingMetadata.class, Proxy.getInvocationHandler(metadata));
        ConnectorTableHandle wrapped = metadata.getTableHandle(null, new SchemaTableName("s", "t"), Optional.empty(), Optional.empty());
        assertInstanceOf(CachingTableHandle.class, wrapped);
        assertSame(stub.metadata.handle, ((CachingTableHandle) wrapped).getDelegateHandle());
    }

    @Test
    void unwrapsDelegateExceptions() {
        StubConnector stub = new StubConnector();
        TrinoException boom = new TrinoException(GENERIC_INTERNAL_ERROR, "boom");
        stub.failure = boom;
        Connector connector = CachingConnector.wrap(stub, Map.of("s.t", List.of("part")), new FileSliceStore(1024 * 1024, null, null, Map.of()), new TableMetadataCache(), new PlanningNotes(null, null), Map.of());

        assertSame(boom, assertThrows(TrinoException.class, connector::shutdown));
    }

    @Test
    void metadataCacheIsSharedAcrossTransactions() {
        StubConnector stub = new StubConnector();
        Connector connector = CachingConnector.wrap(stub, Map.of("s.t", List.of("part")), new FileSliceStore(1024 * 1024, null, null, Map.of()), new TableMetadataCache(), new PlanningNotes(null, null), Map.of("s.t", io.airlift.units.Duration.valueOf("1h")));

        connector.getMetadata(null, null).getTableHandle(null, new SchemaTableName("s", "t"), Optional.empty(), Optional.empty());

        stub.metadata.tableHandleFailure = new RuntimeException("source down");
        // A fresh planning entry is served from the connector-lifetime cache without the delegate.
        ConnectorTableHandle served = connector.getMetadata(null, null) // new transaction, same connector-lifetime cache
                .getTableHandle(null, new SchemaTableName("s", "t"), Optional.empty(), Optional.empty());

        assertInstanceOf(CachingTableHandle.class, served);
        assertSame(stub.metadata.handle, ((CachingTableHandle) served).getDelegateHandle());
    }
}