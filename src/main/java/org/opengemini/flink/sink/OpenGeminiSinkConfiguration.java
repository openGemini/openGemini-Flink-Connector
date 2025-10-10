/*
 * Copyright 2025 openGemini authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opengemini.flink.sink;

import java.io.*;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Configuration for OpenGeminiSink connector. This class encapsulates all configuration options
 * needed to connect and write to OpenGemini.
 *
 * @param <T> The type of elements handled by the sink
 */
public class OpenGeminiSinkConfiguration<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    // Connection configuration
    private final String host;
    private final int port;
    private final String database;
    private final String measurement;
    private final String username;
    private final String password;

    // Batching configuration
    private final int batchSize;
    private final long flushIntervalMillis;

    // Error handling configuration
    private final int maxRetries;

    // Converter configuration
    // MUST be either OpenGeminiLineProtocolConverter or OpenGeminiPointConverter
    private final Object converter;

    // Timeout configuration
    private final Duration connectionTimeout;
    private final Duration requestTimeout;

    // Property names
    public static final String PROP_HOST = "opengemini.connector.host";
    public static final String PROP_PORT = "opengemini.connector.port";
    public static final String PROP_DATABASE = "opengemini.connector.database";
    public static final String PROP_MEASUREMENT = "opengemini.connector.measurement";
    public static final String PROP_USERNAME = "opengemini.connector.username";
    public static final String PROP_PASSWORD = "opengemini.connector.password";
    public static final String PROP_BATCH_SIZE = "opengemini.connector.batch.size";
    public static final String PROP_FLUSH_INTERVAL_MS = "opengemini.connector.flush.interval.ms";
    public static final String PROP_MAX_RETRIES = "opengemini.connector.max.retries";
    public static final String PROP_CONNECTION_TIMEOUT_MS =
            "opengemini.connector.connection.timeout.ms";
    public static final String PROP_REQUEST_TIMEOUT_MS = "opengemini.connector.request.timeout.ms";

    // Default values
    // These values will be overwritten by properties or parameters if provided
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8086;
    public static final int DEFAULT_BATCH_SIZE = 5000;
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 100L;
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final long DEFAULT_CONNECTION_TIMEOUT_MS = 5000L;
    public static final long DEFAULT_REQUEST_TIMEOUT_MS = 30000L;
    public static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 5;
    public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 30;
    public static final String DEFAULT_CONFIG_FILE_NAME = "opengemini-connector.properties";
    public static final String HTTP_PROTOCOL = "http://";
    public static final String HTTPS_PROTOCOL = "https://";
    public static final String COLON = ":";

    private OpenGeminiSinkConfiguration(Builder<T> builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.database = builder.database;
        this.measurement = builder.measurement;
        this.username = builder.username;
        this.password = builder.password;
        this.batchSize = builder.batchSize;
        this.flushIntervalMillis = builder.flushIntervalMillis;
        this.maxRetries = builder.maxRetries;
        this.converter = builder.converter;
        this.connectionTimeout = builder.connectionTimeout;
        this.requestTimeout = builder.requestTimeout;
    }

    /**
     * Creates a new builder from this configuration. This is useful for modifying existing
     * configurations.
     *
     * @return A new builder with values from this configuration
     */
    public Builder<T> toBuilder() {
        Builder<T> builder =
                new Builder<T>()
                        .setHost(this.host)
                        .setPort(this.port)
                        .setDatabase(this.database)
                        .setMeasurement(this.measurement)
                        .setBatchSize(this.batchSize)
                        .setFlushInterval(this.flushIntervalMillis, TimeUnit.MILLISECONDS)
                        .setMaxRetries(this.maxRetries)
                        .setConnectionTimeout(this.connectionTimeout)
                        .setRequestTimeout(this.requestTimeout)
                        .setConverter(this.converter);

        if (this.username != null) {
            builder.setUsername(this.username);
        }
        if (this.password != null) {
            builder.setPassword(this.password);
        }

        return builder;
    }

    /**
     * Creates configuration from ParameterTool.
     *
     * @param params
     * @param converter
     * @return
     * @param <T>
     */
    public static <T> OpenGeminiSinkConfiguration<T> fromParameterTool(
            ParameterTool params, Object converter) {

        // verify required params
        if (!params.has(PROP_DATABASE)) {
            throw new IllegalArgumentException("Missing required parameter: " + PROP_DATABASE);
        }
        if (!params.has(PROP_MEASUREMENT)) {
            throw new IllegalArgumentException("Missing required parameter: " + PROP_MEASUREMENT);
        }

        Builder<T> builder =
                OpenGeminiSinkConfiguration.<T>builder()
                        .setHost(params.get(PROP_HOST, DEFAULT_HOST))
                        .setPort(params.getInt(PROP_PORT, DEFAULT_PORT))
                        .setDatabase(params.getRequired(PROP_DATABASE))
                        .setMeasurement(params.getRequired(PROP_MEASUREMENT))
                        .setBatchSize(params.getInt(PROP_BATCH_SIZE, DEFAULT_BATCH_SIZE))
                        .setFlushInterval(
                                params.getLong(PROP_FLUSH_INTERVAL_MS, DEFAULT_FLUSH_INTERVAL_MS),
                                TimeUnit.MILLISECONDS)
                        .setMaxRetries(params.getInt(PROP_MAX_RETRIES, DEFAULT_MAX_RETRIES))
                        .setConnectionTimeout(
                                Duration.ofMillis(
                                        params.getLong(
                                                PROP_CONNECTION_TIMEOUT_MS,
                                                DEFAULT_CONNECTION_TIMEOUT_MS)))
                        .setRequestTimeout(
                                Duration.ofMillis(
                                        params.getLong(
                                                PROP_REQUEST_TIMEOUT_MS,
                                                DEFAULT_REQUEST_TIMEOUT_MS)))
                        .setConverter(converter);

        // optional params
        if (params.has(PROP_USERNAME)) {
            builder.setUsername(params.get(PROP_USERNAME));
        }
        if (params.has(PROP_PASSWORD)) {
            builder.setPassword(params.get(PROP_PASSWORD));
        }

        return builder.build();
    }

    /**
     * Creates configuration from Properties.
     *
     * @param props
     * @param converter
     * @return
     * @param <T>
     */
    public static <T> OpenGeminiSinkConfiguration<T> fromProperties(
            Properties props, Object converter) {

        // verify required params
        String database = props.getProperty(PROP_DATABASE);
        String measurement = props.getProperty(PROP_MEASUREMENT);

        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("Missing required property: " + PROP_DATABASE);
        }
        if (measurement == null || measurement.isEmpty()) {
            throw new IllegalArgumentException("Missing required property: " + PROP_MEASUREMENT);
        }

        Builder<T> builder =
                OpenGeminiSinkConfiguration.<T>builder()
                        .setHost(props.getProperty(PROP_HOST, DEFAULT_HOST))
                        .setPort(
                                Integer.parseInt(
                                        props.getProperty(PROP_PORT, String.valueOf(DEFAULT_PORT))))
                        .setDatabase(database)
                        .setMeasurement(measurement)
                        .setBatchSize(
                                Integer.parseInt(
                                        props.getProperty(
                                                PROP_BATCH_SIZE,
                                                String.valueOf(DEFAULT_BATCH_SIZE))))
                        .setFlushInterval(
                                Long.parseLong(
                                        props.getProperty(
                                                PROP_FLUSH_INTERVAL_MS,
                                                String.valueOf(DEFAULT_FLUSH_INTERVAL_MS))),
                                TimeUnit.MILLISECONDS)
                        .setMaxRetries(
                                Integer.parseInt(
                                        props.getProperty(
                                                PROP_MAX_RETRIES,
                                                String.valueOf(DEFAULT_MAX_RETRIES))))
                        .setConnectionTimeout(
                                Duration.ofMillis(
                                        Long.parseLong(
                                                props.getProperty(
                                                        PROP_CONNECTION_TIMEOUT_MS,
                                                        String.valueOf(
                                                                DEFAULT_CONNECTION_TIMEOUT_MS)))))
                        .setRequestTimeout(
                                Duration.ofMillis(
                                        Long.parseLong(
                                                props.getProperty(
                                                        PROP_REQUEST_TIMEOUT_MS,
                                                        String.valueOf(
                                                                DEFAULT_REQUEST_TIMEOUT_MS)))))
                        .setConverter(converter);

        // optional authentication params
        String username = props.getProperty(PROP_USERNAME);
        String password = props.getProperty(PROP_PASSWORD);
        if (username != null && !username.isEmpty()) {
            builder.setUsername(username);
        }
        if (password != null && !password.isEmpty()) {
            builder.setPassword(password);
        }

        return builder.build();
    }

    /**
     * Creates configuration from mixed sources: ParameterTool and Properties.
     *
     * @param params
     * @param props
     * @param converter
     * @return
     * @param <T>
     */
    public static <T> OpenGeminiSinkConfiguration<T> fromMixedSources(
            ParameterTool params, Properties props, Object converter) {

        Properties merged = new Properties();

        // 1. if any properties are provided, use them as base
        if (props != null) {
            merged.putAll(props);
        }

        // 2. override with command line parameters
        params.toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith("opengemini.")) {
                                merged.setProperty(key, value);
                            }
                        });

        // 3. create configuration from merged properties
        return fromProperties(merged, converter);
    }

    /**
     * Creates configuration from Properties file.
     *
     * @param propertiesFile The path to the properties file
     * @param converter The point converter
     * @return Configuration instance
     * @throws IOException if reading the file fails
     *     <p>Example usage:
     *     <pre>
     * OpenGeminiSinkConfiguration config =
     *     OpenGeminiSinkConfiguration.fromPropertiesFile("config/opengemini-connector.properties", converter);
     * </pre>
     */
    public static <T> OpenGeminiSinkConfiguration<T> fromPropertiesFile(
            String propertiesFile, Object converter) throws IOException {

        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propertiesFile)) {
            props.load(fis);
        }

        return fromProperties(props, converter);
    }

    /**
     * Creates configuration from Properties file.
     *
     * @param propertiesFile The properties file
     * @param converter The point converter
     * @return Configuration instance
     * @throws IOException if reading the file fails
     */
    public static <T> OpenGeminiSinkConfiguration<T> fromPropertiesFile(
            File propertiesFile, Object converter) throws IOException {

        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propertiesFile)) {
            props.load(fis);
        }

        return fromProperties(props, converter);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getMeasurement() {
        return measurement;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getFlushIntervalMillis() {
        return flushIntervalMillis;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Object getConverter() {
        return converter;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Builder for OpenGeminiSinkConfiguration.
     *
     * @param <T> The type of elements handled by the sink
     */
    public static class Builder<T> {
        // Default values
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private String database;
        private String measurement;
        private String username;
        private String password;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private long flushIntervalMillis = DEFAULT_FLUSH_INTERVAL_MS;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private Object converter;
        private Duration connectionTimeout = Duration.ofSeconds(DEFAULT_CONNECTION_TIMEOUT_SECONDS);
        private Duration requestTimeout = Duration.ofSeconds(DEFAULT_REQUEST_TIMEOUT_SECONDS);

        /**
         * Sets the host address of the OpenGemini server.
         *
         * @param host The host address
         * @return The builder instance
         */
        public Builder<T> setHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the port of the OpenGemini server.
         *
         * @param port The port number
         * @return The builder instance
         */
        public Builder<T> setPort(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the URL of the OpenGemini server. The URL should be in the format: http://host:port
         * or https://host:port<br>
         * Note: This will override any previously set host and port values.
         *
         * @param url The server URL
         * @return The builder instance
         */
        public Builder<T> setUrl(String url) {
            // Parse URL to extract host and port
            try {
                if (url.startsWith(HTTP_PROTOCOL)) {
                    url = url.substring(7);
                } else if (url.startsWith(HTTPS_PROTOCOL)) {
                    url = url.substring(8);
                }

                String[] parts = url.split(COLON);
                if (parts.length >= 1) {
                    this.host = parts[0];
                }
                if (parts.length >= 2) {
                    this.port = Integer.parseInt(parts[1]);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid URL format: " + url);
            }
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param database The database name
         * @return The builder instance
         */
        public Builder<T> setDatabase(String database) {
            this.database = database;
            return this;
        }

        /**
         * Sets the measurement name.
         *
         * @param measurement The measurement name
         * @return The builder instance
         */
        public Builder<T> setMeasurement(String measurement) {
            this.measurement = measurement;
            return this;
        }

        /**
         * Sets the username for authentication.
         *
         * @param username The username
         * @return The builder instance
         */
        public Builder<T> setUsername(String username) {
            this.username = username;
            return this;
        }

        /**
         * Sets the password for authentication.
         *
         * @param password The password
         * @return The builder instance
         */
        public Builder<T> setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the batch size for writing points.
         *
         * @param batchSize The batch size (must be positive)
         * @return The builder instance
         */
        public Builder<T> setBatchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be positive");
            }
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the flush interval for periodic flushing.
         *
         * @param interval The interval value
         * @param unit The time unit
         * @return The builder instance
         */
        public Builder<T> setFlushInterval(long interval, TimeUnit unit) {
            this.flushIntervalMillis = unit.toMillis(interval);
            return this;
        }

        /**
         * Sets the maximum number of retries for failed write attempts.
         *
         * @param maxRetries The maximum number of retries
         * @return The builder instance
         */
        public Builder<T> setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Sets the converter for transforming input elements to OpenGemini points.
         *
         * @param converter The point converter
         * @return The builder instance
         */
        public Builder<T> setConverter(Object converter) {
            this.converter = converter;
            return this;
        }

        /**
         * Sets the connection timeout for HTTP client.
         *
         * @param timeout The connection timeout
         * @return The builder instance
         */
        public Builder<T> setConnectionTimeout(Duration timeout) {
            this.connectionTimeout = timeout;
            return this;
        }

        /**
         * Sets the request timeout for HTTP client.
         *
         * @param timeout The request timeout
         * @return The builder instance
         */
        public Builder<T> setRequestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }

        public Builder<T> host(String host) {
            return setHost(host);
        }

        public Builder<T> port(int port) {
            return setPort(port);
        }

        public Builder<T> database(String database) {
            return setDatabase(database);
        }

        public Builder<T> measurement(String measurement) {
            return setMeasurement(measurement);
        }

        public Builder<T> username(String username) {
            return setUsername(username);
        }

        public Builder<T> password(String password) {
            return setPassword(password);
        }

        public Builder<T> batchSize(int batchSize) {
            return setBatchSize(batchSize);
        }

        public Builder<T> flushIntervalMillis(long millis) {
            return setFlushInterval(millis, TimeUnit.MILLISECONDS);
        }

        public Builder<T> maxRetries(int maxRetries) {
            return setMaxRetries(maxRetries);
        }

        public Builder<T> connectionTimeout(Duration timeout) {
            return setConnectionTimeout(timeout);
        }

        public Builder<T> requestTimeout(Duration timeout) {
            return setRequestTimeout(timeout);
        }

        public Builder<T> converter(Object converter) {
            return setConverter(converter);
        }

        /**
         * Builds the configuration instance.
         *
         * @return The configuration instance
         * @throws IllegalArgumentException if required fields are missing
         */
        public OpenGeminiSinkConfiguration<T> build() {
            if (database == null || database.isEmpty()) {
                throw new IllegalArgumentException("Database must be provided");
            }
            if (measurement == null || measurement.isEmpty()) {
                throw new IllegalArgumentException("Measurement must be provided");
            }

            return new OpenGeminiSinkConfiguration<>(this);
        }
    }

    /**
     * Creates a new builder for OpenGeminiSinkConfiguration.
     *
     * @param <T> The type of elements handled by the sink
     * @return A new builder instance
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Checks if authentication is configured.
     *
     * @return true if both username and password are provided
     */
    public boolean hasAuthentication() {
        return username != null && !username.isEmpty() && password != null && !password.isEmpty();
    }
}
