package org.fleetmap;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    private static final Properties properties = new Properties();
    static {
        try (InputStream input = S3OutputFile.class.getClassLoader().getResourceAsStream("athena-storage.properties")) {
            if (input != null) {
                properties.load(input);
            } else {
                throw new RuntimeException("config.properties not found in classpath");
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to load config.properties", ex);
        }
    }

    public static String getBucket() {
        return properties.getProperty("bucket", "traccar-positions");
    }

    public static String getTable() {
        return properties.getProperty("table", "traccar_positions");
    }

    public static String getDatabase() {
        return properties.getProperty("database", "traccar_positions");
    }

    // seconds
    public static int getFlushInterval() {
        return 1;
    }
}
