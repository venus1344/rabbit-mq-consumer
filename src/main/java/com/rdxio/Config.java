package com.rdxio;

import java.io.InputStream;
import java.util.Properties;

public class Config {
    private static final Properties properties;

    static {
        try (InputStream input = Config.class.getClassLoader().getResourceAsStream("application.properties")) {
            properties = new Properties();
            properties.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Error loading configuration", e);
        }
    }

    public static String get(String key) {
        return properties.getProperty(key);
    }
} 