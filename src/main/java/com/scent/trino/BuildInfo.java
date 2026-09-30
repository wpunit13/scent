package com.scent.trino;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Build-time metadata baked into the plugin by Maven resource filtering.
 */
public final class BuildInfo {

    private static final String TRINO_VERSION = loadTrinoVersion();

    private BuildInfo() {}

    /**
     * The Trino version this plugin was built against. The bundled delegate connectors and the
     * plugin's own SPI usage are only valid on a cluster running this exact version.
     */
    public static String trinoVersion() {
        return TRINO_VERSION;
    }

    private static String loadTrinoVersion() {
        try (InputStream input = BuildInfo.class.getClassLoader().getResourceAsStream("scent-trino-plugin.properties")) {
            if (input == null) {
                return "unknown";
            }
            Properties properties = new Properties();
            properties.load(input);
            return properties.getProperty("trino.version", "unknown");
        }
        catch (IOException e) {
            return "unknown";
        }
    }
}
