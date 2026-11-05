package com.scent.trino.cache;

import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLException;

/**
 * Distinguishes "the delegate source is unreachable" from every other failure, so outage
 * fallbacks never mask real bugs.
 *
 * <p>The delegate's connectivity failures arrive as EXTERNAL-type TrinoExceptions (JDBC errors
 * report type EXTERNAL; the JDBC_ERROR enum itself lives in the engine's base-jdbc module, which
 * a plugin cannot reference), or as one of the JDK's standard connectivity exceptions somewhere
 * in the cause chain. Anything else — NPE, ClassCastException, ... — is a bug and must propagate.
 */
final class SourceAvailability {

    private SourceAvailability() {}

    static boolean isSourceUnavailable(Throwable error) {
        if (error instanceof TrinoException trinoException
                && trinoException.getErrorCode().getType() == ErrorType.EXTERNAL) {
            return true;
        }
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof ConnectException
                    || t instanceof SocketTimeoutException
                    || t instanceof UnknownHostException
                    || t instanceof SSLException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }
}
