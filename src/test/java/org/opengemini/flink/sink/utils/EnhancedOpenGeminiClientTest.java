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
package org.opengemini.flink.sink.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengemini.flink.utils.EnhancedOpenGeminiClient;

import io.github.openfacade.http.HttpClientConfig;
import io.opengemini.client.api.Address;
import io.opengemini.client.api.Configuration;

class EnhancedOpenGeminiClientTest {

    static class TestableEnhancedOpenGeminiClient extends EnhancedOpenGeminiClient {
        private boolean executeWriteCalled = false;
        private String lastDatabase;
        private String lastRetentionPolicy;
        private String lastBody;

        public TestableEnhancedOpenGeminiClient(Configuration conf) {
            super(conf);
        }

        @Override
        protected CompletableFuture<Void> executeWrite(
                String database, String retentionPolicy, String body) {
            this.executeWriteCalled = true;
            this.lastDatabase = database;
            this.lastRetentionPolicy = retentionPolicy;
            this.lastBody = body;
            return CompletableFuture.completedFuture(null);
        }

        // Getters for verification
        public boolean wasExecuteWriteCalled() {
            return executeWriteCalled;
        }

        public String getLastDatabase() {
            return lastDatabase;
        }

        public String getLastRetentionPolicy() {
            return lastRetentionPolicy;
        }

        public String getLastBody() {
            return lastBody;
        }

        public void reset() {
            executeWriteCalled = false;
            lastDatabase = null;
            lastRetentionPolicy = null;
            lastBody = null;
        }
    }

    private TestableEnhancedOpenGeminiClient client;
    private Configuration configuration;

    @BeforeEach
    void setUp() {
        // Create real configuration - no mocking needed
        HttpClientConfig httpConfig =
                new HttpClientConfig.Builder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .timeout(Duration.ofSeconds(30))
                        .build();

        configuration =
                Configuration.builder()
                        .addresses(Arrays.asList(new Address("localhost", 8086)))
                        .httpConfig(httpConfig)
                        .build();

        client = new TestableEnhancedOpenGeminiClient(configuration);
    }

    @Test
    void testWriteLineProtocols_DatabaseNull_ThrowsException() {
        List<String> lineProtocols = Arrays.asList("measurement,tag=value field=1");

        CompletableFuture<Void> result = client.writeLineProtocols(null, "policy", lineProtocols);

        assertTrue(result.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, result::get);
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        assertEquals("Database name cannot be null or empty", exception.getCause().getMessage());
        assertFalse(client.wasExecuteWriteCalled());
    }

    @Test
    void testWriteLineProtocols_DatabaseEmpty_ThrowsException() {
        List<String> lineProtocols = Arrays.asList("measurement,tag=value field=1");

        CompletableFuture<Void> result = client.writeLineProtocols("", "policy", lineProtocols);

        assertTrue(result.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, result::get);
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        assertFalse(client.wasExecuteWriteCalled());
    }

    @Test
    void testWriteLineProtocols_DatabaseBlank_ThrowsException() {
        List<String> lineProtocols = Arrays.asList("measurement,tag=value field=1");

        CompletableFuture<Void> result = client.writeLineProtocols("   ", "policy", lineProtocols);

        assertTrue(result.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, result::get);
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        assertFalse(client.wasExecuteWriteCalled());
    }

    @Test
    void testWriteLineProtocols_LineProtocolsNull_ReturnsCompletedFuture() throws Exception {
        CompletableFuture<Void> result = client.writeLineProtocols("testdb", "policy", null);

        assertTrue(result.isDone());
        assertNull(result.get());
        assertFalse(client.wasExecuteWriteCalled());
    }

    @Test
    void testWriteLineProtocols_LineProtocolsEmpty_ReturnsCompletedFuture() throws Exception {
        List<String> emptyList = Collections.emptyList();

        CompletableFuture<Void> result = client.writeLineProtocols("testdb", "policy", emptyList);

        assertTrue(result.isDone());
        assertNull(result.get());
        assertFalse(client.wasExecuteWriteCalled());
    }

    @Test
    void testWriteLineProtocols_ValidData_CallsExecuteWrite() throws Exception {
        List<String> lineProtocols =
                Arrays.asList("measurement1,tag=value1 field=1", "measurement2,tag=value2 field=2");

        CompletableFuture<Void> result =
                client.writeLineProtocols("testdb", "policy", lineProtocols);

        assertNotNull(result);
        assertTrue(result.isDone());
        assertTrue(client.wasExecuteWriteCalled());
        assertEquals("testdb", client.getLastDatabase());
        assertEquals("policy", client.getLastRetentionPolicy());
        assertEquals(
                "measurement1,tag=value1 field=1\nmeasurement2,tag=value2 field=2",
                client.getLastBody());
    }

    @Test
    void testWriteLineProtocols_WithEmptyStrings_FiltersEmptyLines() {
        List<String> lineProtocols =
                Arrays.asList(
                        "measurement1,tag=value1 field=1",
                        "",
                        "measurement2,tag=value2 field=2",
                        null);

        client.writeLineProtocols("testdb", "policy", lineProtocols);

        assertTrue(client.wasExecuteWriteCalled());
        assertEquals(
                "measurement1,tag=value1 field=1\nmeasurement2,tag=value2 field=2",
                client.getLastBody());
    }

    @Test
    void testWriteLineProtocols_OnlyEmptyStrings_ReturnsCompletedFuture() throws Exception {
        List<String> lineProtocols = Arrays.asList("", "   ", null);

        CompletableFuture<Void> result =
                client.writeLineProtocols("testdb", "policy", lineProtocols);

        assertTrue(result.isDone());
        assertNull(result.get());
        assertFalse(client.wasExecuteWriteCalled());
    }

    @Test
    void testWriteLineProtocols_TrimsWhitespace() {
        List<String> lineProtocols =
                Arrays.asList(
                        "  measurement1,tag=value1 field=1  ",
                        "\tmeasurement2,tag=value2 field=2\n");

        client.writeLineProtocols("testdb", "policy", lineProtocols);

        assertTrue(client.wasExecuteWriteCalled());
        assertEquals(
                "measurement1,tag=value1 field=1\nmeasurement2,tag=value2 field=2",
                client.getLastBody());
    }

    @Test
    void testWriteLineProtocols_NullRetentionPolicy() {
        List<String> lineProtocols = Arrays.asList("measurement,tag=value field=1");

        client.writeLineProtocols("testdb", null, lineProtocols);

        assertTrue(client.wasExecuteWriteCalled());
        assertEquals("testdb", client.getLastDatabase());
        assertNull(client.getLastRetentionPolicy());
        assertEquals("measurement,tag=value field=1", client.getLastBody());
    }

    @Test
    void testWriteLineProtocols_SingleLineProtocol() {
        List<String> lineProtocols = Arrays.asList("measurement,tag=value field=1.5");

        client.writeLineProtocols("testdb", "retention_policy", lineProtocols);

        assertTrue(client.wasExecuteWriteCalled());
        assertEquals("testdb", client.getLastDatabase());
        assertEquals("retention_policy", client.getLastRetentionPolicy());
        assertEquals("measurement,tag=value field=1.5", client.getLastBody());
    }
}
