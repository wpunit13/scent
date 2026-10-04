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
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingConnectorTest {

    private static final class StubMetadata implements ConnectorMetadata {
        final ConnectorTableHandle handle = new ConnectorTableHandle() {};

        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session,
                SchemaTableName tableName,
                Optional<ConnectorTableVersion> startVersion,
                Optional<ConnectorTableVersion> endVersion) {
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
        Connector connector = CachingConnector.wrap(stub, "part", new InMemorySliceStore(1024 * 1024));

        connector.shutdown();

        assertTrue(stub.shutdown);
    }

    @Test
    void wrapsSpiSubObjectsAndForwardsCalls() {
        StubConnector stub = new StubConnector();
        Connector connector = CachingConnector.wrap(stub, "part", new InMemorySliceStore(1024 * 1024));

        assertInstanceOf(CachingSplitManager.class, connector.getSplitManager());
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
        Connector connector = CachingConnector.wrap(stub, "part", new InMemorySliceStore(1024 * 1024));

        assertSame(boom, assertThrows(TrinoException.class, connector::shutdown));
    }
}