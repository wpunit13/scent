package com.scent.trino.cache;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLException;

/**
 * Distinguishes "the delegate source is unreachable" from every other failure, so outage
 * fallbacks never mask real bugs.
 *
 * <p>A source outage is recognized only by a JDK connectivity exception in the cause chain
 * (ConnectException, SocketTimeoutException, UnknownHostException, SSLException). Anything else —
 * permission-denied, constraint violation, NPE, ClassCastException, ... — is not an outage and must
 * propagate, so a non-connectivity failure is never masked by replaying a stale split batch.
 */
final class SourceAvailability {

    private SourceAvailability() {}

    static boolean isSourceUnavailable(Throwable error) {
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
