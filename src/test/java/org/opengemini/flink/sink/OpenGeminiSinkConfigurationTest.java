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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class OpenGeminiSinkConfigurationTest {

    @Mock private OpenGeminiPointConverter<String> mockConverter;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testBuildWithAllFields() {

        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setHost("example.com")
                        .setPort(1234)
                        .setUrl("http://localhost:8089")
                        .setDatabase("testDB")
                        .setMeasurement("testMeasurement")
                        .setUsername("user")
                        .setPassword("pass")
                        .setBatchSize(1000)
                        .setFlushInterval(200, TimeUnit.MILLISECONDS)
                        .setMaxRetries(5)
                        .setConnectionTimeout(Duration.ofSeconds(10))
                        .setRequestTimeout(Duration.ofSeconds(20))
                        .setConverter(mockConverter)
                        .build();

        assertEquals("localhost", config.getHost());
        assertEquals(8089, config.getPort());
        assertEquals("testDB", config.getDatabase());
        assertEquals("testMeasurement", config.getMeasurement());
        assertEquals("user", config.getUsername());
        assertEquals("pass", config.getPassword());
        assertEquals(1000, config.getBatchSize());
        assertEquals(200, config.getFlushIntervalMillis());
        assertEquals(5, config.getMaxRetries());
        assertEquals(Duration.ofSeconds(10), config.getConnectionTimeout());
        assertEquals(Duration.ofSeconds(20), config.getRequestTimeout());
        assertTrue(config.hasAuthentication());
    }

    @Test
    void testMissingDatabaseThrows() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                OpenGeminiSinkConfiguration.<String>builder()
                                        .setMeasurement("m")
                                        .setConverter(mockConverter)
                                        .build());
        assertEquals("Database must be provided", ex.getMessage());
    }

    @Test
    void testMissingMeasurementThrows() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                OpenGeminiSinkConfiguration.<String>builder()
                                        .setDatabase("db")
                                        .setConverter(mockConverter)
                                        .build());
        assertEquals("Measurement must be provided", ex.getMessage());
    }

    @Test
    void testMissingConverterThrows() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                OpenGeminiSinkConfiguration.<String>builder()
                                        .setDatabase("db")
                                        .setMeasurement("m")
                                        .build());
        assertEquals("Converter must be provided", ex.getMessage());
    }

    @Test
    void testInvalidBatchSizeThrows() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                OpenGeminiSinkConfiguration.<String>builder()
                                        .setDatabase("db")
                                        .setMeasurement("m")
                                        .setConverter(mockConverter)
                                        .setBatchSize(0)
                                        .build());
        assertEquals("Batch size must be positive", ex.getMessage());
    }

    @Test
    void testHasAuthenticationFalse() {
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setConverter(mockConverter)
                        .build();

        assertFalse(config.hasAuthentication());
    }

    @Test
    void testSetUrlWithHttpPrefix() {
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setUrl("http://myhost:9999")
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setConverter(mockConverter)
                        .build();

        assertEquals("myhost", config.getHost());
        assertEquals(9999, config.getPort());
    }

    @Test
    void testSetUrlWithHttpsPrefix() {
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setUrl("https://secure.host:443")
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setConverter(mockConverter)
                        .build();

        assertEquals("secure.host", config.getHost());
        assertEquals(443, config.getPort());
    }

    @Test
    void testSetUrlWithoutPort() {
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setUrl("myhost")
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setConverter(mockConverter)
                        .build();

        assertEquals("myhost", config.getHost());
        assertEquals(8086, config.getPort()); // Should keep default port
    }

    @Test
    void testSetUrlWithInvalidFormat() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        OpenGeminiSinkConfiguration.<String>builder()
                                .setUrl("http://host:invalid-port")
                                .build(),
                "Invalid URL format: http://host:invalid-port");
    }

    @Test
    void testNegativeBatchSizeThrows() {
        assertThrows(
                IllegalArgumentException.class,
                () -> OpenGeminiSinkConfiguration.<String>builder().setBatchSize(-1).build(),
                "Batch size must be positive");
    }

    @Test
    void testSetPointConverterAlias() {
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setPointConverter(mockConverter) // Using alias method
                        .build();

        assertEquals(mockConverter, config.getConverter());
    }

    @Test
    void testHasAuthenticationTrue() {
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setUsername("user")
                        .setPassword("pass")
                        .setConverter(mockConverter)
                        .build();

        assertTrue(config.hasAuthentication());
    }

    @Test
    void testHasAuthenticationWithEmptyUsername() {
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setUsername("")
                        .setPassword("pass")
                        .setConverter(mockConverter)
                        .build();

        assertFalse(config.hasAuthentication());
    }

    @Test
    void testHasAuthenticationWithEmptyPassword() {
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setUsername("user")
                        .setPassword("")
                        .setConverter(mockConverter)
                        .build();

        assertFalse(config.hasAuthentication());
    }

    @Test
    void testDefaultValues() {
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setConverter(mockConverter)
                        .build();

        assertEquals("localhost", config.getHost());
        assertEquals(8086, config.getPort());
        assertEquals(5000, config.getBatchSize());
        assertEquals(100, config.getFlushIntervalMillis());
        assertEquals(3, config.getMaxRetries());
        assertEquals(Duration.ofSeconds(5), config.getConnectionTimeout());
        assertEquals(Duration.ofSeconds(30), config.getRequestTimeout());
        assertNull(config.getUsername());
        assertNull(config.getPassword());
    }

    @Test
    void testFlushIntervalWithDifferentTimeUnits() {
        OpenGeminiSinkConfiguration<String> config1 =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setConverter(mockConverter)
                        .setFlushInterval(2, TimeUnit.SECONDS)
                        .build();
        assertEquals(2000, config1.getFlushIntervalMillis());

        OpenGeminiSinkConfiguration<String> config2 =
                OpenGeminiSinkConfiguration.<String>builder()
                        .setDatabase("db")
                        .setMeasurement("m")
                        .setConverter(mockConverter)
                        .setFlushInterval(1, TimeUnit.MINUTES)
                        .build();
        assertEquals(60000, config2.getFlushIntervalMillis());
    }

    @Test
    void testEmptyDatabaseThrows() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        OpenGeminiSinkConfiguration.<String>builder()
                                .setDatabase("")
                                .setMeasurement("m")
                                .setConverter(mockConverter)
                                .build(),
                "Database must be provided");
    }

    @Test
    void testEmptyMeasurementThrows() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        OpenGeminiSinkConfiguration.<String>builder()
                                .setDatabase("db")
                                .setMeasurement("")
                                .setConverter(mockConverter)
                                .build(),
                "Measurement must be provided");
    }

    @Test
    void testFromParameterToolWithAllParameters() {
        Map<String, String> args = new HashMap<>();
        args.put("opengemini.connector.host", "testhost");
        args.put("opengemini.connector.port", "9999");
        args.put("opengemini.connector.database", "testdb");
        args.put("opengemini.connector.measurement", "testmeas");
        args.put("opengemini.connector.username", "testuser");
        args.put("opengemini.connector.password", "testpass");
        args.put("opengemini.connector.batch.size", "2000");
        args.put("opengemini.connector.flush.interval.ms", "500");
        args.put("opengemini.connector.max.retries", "10");
        args.put("opengemini.connector.connection.timeout.ms", "8000");
        args.put("opengemini.connector.request.timeout.ms", "60000");

        ParameterTool params = ParameterTool.fromMap(args);

        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.fromParameterTool(params, mockConverter);

        assertEquals("testhost", config.getHost());
        assertEquals(9999, config.getPort());
        assertEquals("testdb", config.getDatabase());
        assertEquals("testmeas", config.getMeasurement());
        assertEquals("testuser", config.getUsername());
        assertEquals("testpass", config.getPassword());
        assertEquals(2000, config.getBatchSize());
        assertEquals(500, config.getFlushIntervalMillis());
        assertEquals(10, config.getMaxRetries());
        assertEquals(Duration.ofMillis(8000), config.getConnectionTimeout());
        assertEquals(Duration.ofMillis(60000), config.getRequestTimeout());
        assertEquals(mockConverter, config.getConverter());
    }

    @Test
    void testFromParameterToolWithDefaultValues() {
        Map<String, String> args = new HashMap<>();
        args.put("opengemini.connector.database", "db");
        args.put("opengemini.connector.measurement", "m");

        ParameterTool params = ParameterTool.fromMap(args);

        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.fromParameterTool(params, mockConverter);

        assertEquals("localhost", config.getHost());
        assertEquals(8086, config.getPort());
        assertEquals(5000, config.getBatchSize());
        assertEquals(100, config.getFlushIntervalMillis());
        assertEquals(3, config.getMaxRetries());
        assertEquals(Duration.ofMillis(5000), config.getConnectionTimeout());
        assertEquals(Duration.ofMillis(30000), config.getRequestTimeout());
        assertNull(config.getUsername());
        assertNull(config.getPassword());
    }

    @Test
    void testFromParameterToolMissingDatabase() {
        Map<String, String> args = new HashMap<>();
        args.put("opengemini.connector.measurement", "m");

        ParameterTool params = ParameterTool.fromMap(args);

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> OpenGeminiSinkConfiguration.fromParameterTool(params, mockConverter));
        assertEquals("Missing required parameter: opengemini.connector.database", ex.getMessage());
    }

    @Test
    void testFromParameterToolMissingMeasurement() {
        Map<String, String> args = new HashMap<>();
        args.put("opengemini.connector.database", "db");

        ParameterTool params = ParameterTool.fromMap(args);

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> OpenGeminiSinkConfiguration.fromParameterTool(params, mockConverter));
        assertEquals(
                "Missing required parameter: opengemini.connector.measurement", ex.getMessage());
    }

    @Test
    void testFromParameterToolWithoutAuthentication() {
        Map<String, String> args = new HashMap<>();
        args.put("opengemini.connector.database", "db");
        args.put("opengemini.connector.measurement", "m");

        ParameterTool params = ParameterTool.fromMap(args);

        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.fromParameterTool(params, mockConverter);

        assertFalse(config.hasAuthentication());
    }

    @Test
    void testFromPropertiesWithAllParameters() {
        Properties props = new Properties();
        props.setProperty("opengemini.connector.host", "prophost");
        props.setProperty("opengemini.connector.port", "7777");
        props.setProperty("opengemini.connector.database", "propdb");
        props.setProperty("opengemini.connector.measurement", "propmeas");
        props.setProperty("opengemini.connector.username", "propuser");
        props.setProperty("opengemini.connector.password", "proppass");
        props.setProperty("opengemini.connector.batch.size", "3000");
        props.setProperty("opengemini.connector.flush.interval.ms", "250");
        props.setProperty("opengemini.connector.max.retries", "7");
        props.setProperty("opengemini.connector.connection.timeout.ms", "12000");
        props.setProperty("opengemini.connector.request.timeout.ms", "45000");

        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.fromProperties(props, mockConverter);

        assertEquals("prophost", config.getHost());
        assertEquals(7777, config.getPort());
        assertEquals("propdb", config.getDatabase());
        assertEquals("propmeas", config.getMeasurement());
        assertEquals("propuser", config.getUsername());
        assertEquals("proppass", config.getPassword());
        assertEquals(3000, config.getBatchSize());
        assertEquals(250, config.getFlushIntervalMillis());
        assertEquals(7, config.getMaxRetries());
        assertEquals(Duration.ofMillis(12000), config.getConnectionTimeout());
        assertEquals(Duration.ofMillis(45000), config.getRequestTimeout());
    }

    @Test
    void testFromPropertiesWithDefaultValues() {
        Properties props = new Properties();
        props.setProperty("opengemini.connector.database", "db");
        props.setProperty("opengemini.connector.measurement", "m");

        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.fromProperties(props, mockConverter);

        assertEquals("localhost", config.getHost());
        assertEquals(8086, config.getPort());
        assertEquals(5000, config.getBatchSize());
        assertEquals(100, config.getFlushIntervalMillis());
        assertEquals(3, config.getMaxRetries());
        assertEquals(Duration.ofMillis(5000), config.getConnectionTimeout());
        assertEquals(Duration.ofMillis(30000), config.getRequestTimeout());
    }

    @Test
    void testFromPropertiesMissingDatabase() {
        Properties props = new Properties();
        props.setProperty("opengemini.connector.measurement", "m");

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> OpenGeminiSinkConfiguration.fromProperties(props, mockConverter));
        assertEquals("Missing required property: opengemini.connector.database", ex.getMessage());
    }

    @Test
    void testFromPropertiesMissingMeasurement() {
        Properties props = new Properties();
        props.setProperty("opengemini.connector.database", "db");

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> OpenGeminiSinkConfiguration.fromProperties(props, mockConverter));
        assertEquals(
                "Missing required property: opengemini.connector.measurement", ex.getMessage());
    }

    @Test
    void testFromPropertiesEmptyDatabase() {
        Properties props = new Properties();
        props.setProperty("opengemini.connector.database", "");
        props.setProperty("opengemini.connector.measurement", "m");

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> OpenGeminiSinkConfiguration.fromProperties(props, mockConverter));
        assertEquals("Missing required property: opengemini.connector.database", ex.getMessage());
    }

    @Test
    void testFromPropertiesEmptyMeasurement() {
        Properties props = new Properties();
        props.setProperty("opengemini.connector.database", "db");
        props.setProperty("opengemini.connector.measurement", "");

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> OpenGeminiSinkConfiguration.fromProperties(props, mockConverter));
        assertEquals(
                "Missing required property: opengemini.connector.measurement", ex.getMessage());
    }

    @Test
    void testFromPropertiesWithEmptyAuthentication() {
        Properties props = new Properties();
        props.setProperty("opengemini.connector.database", "db");
        props.setProperty("opengemini.connector.measurement", "m");
        props.setProperty("opengemini.connector.username", "");
        props.setProperty("opengemini.connector.password", "");

        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.fromProperties(props, mockConverter);

        assertNull(config.getUsername());
        assertNull(config.getPassword());
        assertFalse(config.hasAuthentication());
    }

    @Test
    void testFromPropertiesInvalidNumberFormat() {
        Properties props = new Properties();
        props.setProperty("opengemini.connector.database", "db");
        props.setProperty("opengemini.connector.measurement", "m");
        props.setProperty("opengemini.connector.port", "invalid");

        assertThrows(
                NumberFormatException.class,
                () -> OpenGeminiSinkConfiguration.fromProperties(props, mockConverter));
    }

    @Test
    void testFromPropertiesInvalidBatchSize() {
        Properties props = new Properties();
        props.setProperty("opengemini.connector.database", "db");
        props.setProperty("opengemini.connector.measurement", "m");
        props.setProperty("opengemini.connector.batch.size", "abc");

        assertThrows(
                NumberFormatException.class,
                () -> OpenGeminiSinkConfiguration.fromProperties(props, mockConverter));
    }

    @Test
    void testFromPropertiesFileString(@TempDir Path tempDir) throws IOException {
        // Create properties file
        File propertiesFile = tempDir.resolve("opengemini-connector.properties").toFile();

        Properties props = new Properties();
        props.setProperty("opengemini.connector.host", "filehost");
        props.setProperty("opengemini.connector.port", "8888");
        props.setProperty("opengemini.connector.database", "filedb");
        props.setProperty("opengemini.connector.measurement", "filemeas");
        props.setProperty("opengemini.connector.username", "fileuser");
        props.setProperty("opengemini.connector.password", "filepass");
        props.setProperty("opengemini.connector.batch.size", "4000");
        props.setProperty("opengemini.connector.flush.interval.ms", "300");

        try (FileOutputStream fos = new FileOutputStream(propertiesFile)) {
            props.store(fos, "Test Configuration");
        }

        // Test loading from file path string
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.fromPropertiesFile(
                        propertiesFile.getAbsolutePath(), mockConverter);

        assertEquals("filehost", config.getHost());
        assertEquals(8888, config.getPort());
        assertEquals("filedb", config.getDatabase());
        assertEquals("filemeas", config.getMeasurement());
        assertEquals("fileuser", config.getUsername());
        assertEquals("filepass", config.getPassword());
        assertEquals(4000, config.getBatchSize());
        assertEquals(300, config.getFlushIntervalMillis());
    }

    @Test
    void testFromPropertiesFileObject(@TempDir Path tempDir) throws IOException {
        // Create properties file
        File propertiesFile = tempDir.resolve("test.properties").toFile();

        Properties props = new Properties();
        props.setProperty("opengemini.connector.database", "testdb");
        props.setProperty("opengemini.connector.measurement", "testmeas");
        // Only required fields, use defaults for others

        try (FileOutputStream fos = new FileOutputStream(propertiesFile)) {
            props.store(fos, null);
        }

        // Test loading from File object
        OpenGeminiSinkConfiguration<String> config =
                OpenGeminiSinkConfiguration.fromPropertiesFile(propertiesFile, mockConverter);

        assertEquals("testdb", config.getDatabase());
        assertEquals("testmeas", config.getMeasurement());
        // Check defaults
        assertEquals("localhost", config.getHost());
        assertEquals(8086, config.getPort());
    }

    @Test
    void testFromPropertiesFileInvalidContent(@TempDir Path tempDir) throws IOException {
        // Create properties file with missing required fields
        File propertiesFile = tempDir.resolve("invalid.properties").toFile();

        Properties props = new Properties();
        props.setProperty("opengemini.connector.host", "localhost");
        // Missing required database and measurement

        try (FileOutputStream fos = new FileOutputStream(propertiesFile)) {
            props.store(fos, null);
        }

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        OpenGeminiSinkConfiguration.fromPropertiesFile(
                                propertiesFile, mockConverter));
    }

    @Test
    void testCreateDefaultConfigurationFromClasspath() throws IOException {
        // This test assumes you have a test properties file in src/test/resources
        // Create a test properties file in src/test/resources/opengemini-connector.properties
        // with the following content:
        // opengemini.database=classpathdb
        // opengemini.measurement=classpathmeas

        // For unit testing, we'll skip this test if the file doesn't exist
        InputStream is =
                getClass()
                        .getClassLoader()
                        .getResourceAsStream(OpenGeminiSinkConfiguration.DEFAULT_CONFIG_FILE_NAME);

        if (is != null) {
            is.close();

            OpenGeminiSinkConfiguration<String> config =
                    OpenGeminiSinkConfiguration.createDefaultConfiguration(mockConverter);

            assertNotNull(config.getDatabase());
            assertNotNull(config.getMeasurement());
        } else {
            // Skip test if no classpath file exists
            System.out.println("Skipping classpath test - no properties file in classpath");
        }
    }
}
