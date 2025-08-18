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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import io.github.openfacade.http.HttpClientConfig;
import io.opengemini.client.api.Point;
import io.opengemini.client.impl.OpenGeminiClient;
import io.opengemini.client.impl.OpenGeminiClientFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class OpenGeminiSinkTest {

    @Mock private OpenGeminiClient mockClient;

    @Mock private OpenGeminiPointConverter<TestData> mockConverter;

    @Mock private FunctionInitializationContext mockInitContext;

    @Mock private OperatorStateStore mockOperatorStateStore;
    @Mock private FunctionSnapshotContext mockSnapshotContext;

    @Mock private ListState<List<Point>> mockListState;
    @Mock private RuntimeContext mockRuntimeContext;
    @Mock private OperatorMetricGroup mockMetricGroup;
    @Mock private Counter mockCounter;

    private OpenGeminiSink<TestData> sink;
    private OpenGeminiSinkConfiguration<TestData> configuration;
    public static final String TEST_DB_NAME = "test_db";
    public static final String TEST_MEASUREMENT_NAME = "test_measurement";
    public static final String TEST_USERNAME = "testuser";
    public static final String TEST_PASSWORD = "testpass";

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        configuration =
                OpenGeminiSinkConfiguration.<TestData>builder()
                        .setHost(OpenGeminiSinkConfiguration.DEFAULT_HOST)
                        .setPort(OpenGeminiSinkConfiguration.DEFAULT_PORT)
                        .setDatabase(TEST_DB_NAME)
                        .setMeasurement(TEST_MEASUREMENT_NAME)
                        .setBatchSize(OpenGeminiSinkConfiguration.DEFAULT_BATCH_SIZE)
                        .setFlushInterval(
                                OpenGeminiSinkConfiguration.DEFAULT_FLUSH_INTERVAL_MS,
                                TimeUnit.MILLISECONDS)
                        .setMaxRetries(OpenGeminiSinkConfiguration.DEFAULT_MAX_RETRIES)
                        .setConnectionTimeout(
                                Duration.ofSeconds(
                                        OpenGeminiSinkConfiguration.DEFAULT_CONNECTION_TIMEOUT_MS))
                        .setRequestTimeout(
                                Duration.ofSeconds(
                                        OpenGeminiSinkConfiguration
                                                .DEFAULT_REQUEST_TIMEOUT_SECONDS))
                        .setConverter(mockConverter)
                        .build();

        sink = createTestSink(configuration);

        // Setup metrics mocks
        when(mockRuntimeContext.getMetricGroup()).thenReturn(mockMetricGroup);
        when(mockMetricGroup.counter(anyString())).thenReturn(mockCounter);
        when(mockMetricGroup.gauge(anyString(), any(Gauge.class)))
                .thenAnswer(
                        invocation ->
                                invocation.getArgument(1)); // Return the gauge that was passed in
        when(mockMetricGroup.histogram(anyString(), any()))
                .thenReturn(mock(org.apache.flink.metrics.Histogram.class));

        // Make addGroup return the same mock so chaining works
        when(mockMetricGroup.addGroup(anyString())).thenReturn(mockMetricGroup);

        when(mockInitContext.getOperatorStateStore()).thenReturn(mockOperatorStateStore);
        when(mockOperatorStateStore.getListState(any(ListStateDescriptor.class)))
                .thenReturn(mockListState);
        when(mockInitContext.isRestored()).thenReturn(false);
        when(mockListState.get()).thenReturn(Collections.emptyList());
    }

    @Test
    void testOpenInitializesClientAndResources() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            // Act
            sink.open(new Configuration());

            // Verify
            mockedFactory.verify(() -> OpenGeminiClientFactory.create(any()), times(1));
            verify(mockClient).createDatabase(TEST_DB_NAME);
        }
    }

    @Test
    void testInvokeWithNullValue() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            sink.open(new Configuration());

            // Act - should not throw exception
            assertDoesNotThrow(() -> sink.invoke(null, mock(SinkFunction.Context.class)));

            // Verify converter was not called
            verify(mockConverter, never()).convert(any(), anyString());
        }
    }

    @Test
    void testBatchingTriggersWhenBatchSizeReached() throws Exception {

        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);

            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.write(anyString(), anyList()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Create configuration with small batch size
            configuration =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost(OpenGeminiSinkConfiguration.DEFAULT_HOST)
                            .setPort(OpenGeminiSinkConfiguration.DEFAULT_PORT)
                            .setDatabase(TEST_DB_NAME)
                            .setMeasurement(TEST_MEASUREMENT_NAME)
                            .setBatchSize(2)
                            .setFlushInterval(
                                    10,
                                    TimeUnit.SECONDS) // Long interval to ensure batch size triggers
                            .setConverter(mockConverter)
                            .build();

            sink = createTestSink(configuration);
            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Create test data
            List<TestData> testDataList =
                    Arrays.asList(new TestData("sensor1", 25.5), new TestData("sensor2", 26.0));

            // Setup converter
            when(mockConverter.convert(any(), anyString()))
                    .thenAnswer(
                            invocation -> {
                                TestData data = invocation.getArgument(0);
                                return createMockPoint(data.sensorId, data.value);
                            });

            // Act - invoke twice to trigger batch
            for (TestData data : testDataList) {
                sink.invoke(data, mock(SinkFunction.Context.class));
            }

            // Verify batch was written
            verify(mockClient, atLeastOnce()).write(eq(TEST_DB_NAME), anyList());
        }
    }

    @Test
    void testFlushIntervalTrigger() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.write(anyString(), anyList()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Create configuration with short flush interval
            configuration =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost(OpenGeminiSinkConfiguration.DEFAULT_HOST)
                            .setPort(OpenGeminiSinkConfiguration.DEFAULT_PORT)
                            .setDatabase(TEST_DB_NAME)
                            .setMeasurement(TEST_MEASUREMENT_NAME)
                            .setBatchSize(100) // Large batch size to ensure interval triggers first
                            .setFlushInterval(50, TimeUnit.MILLISECONDS)
                            .setConverter(mockConverter)
                            .build();

            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Add one point
            TestData testData = new TestData("sensor1", 25.5);
            Point mockPoint = createMockPoint("sensor1", 25.5);
            when(mockConverter.convert(testData, TEST_MEASUREMENT_NAME)).thenReturn(mockPoint);

            sink.invoke(testData, mock(SinkFunction.Context.class));

            // Wait for flush interval
            Thread.sleep(configuration.getFlushIntervalMillis() + 50);

            // call invoke again to trigger flushing
            sink.invoke(testData, mock(SinkFunction.Context.class));

            // Verify flush occurred
            verify(mockClient, atLeastOnce()).write(eq(TEST_DB_NAME), anyList());
        }
    }

    @Test
    void testRetryMechanism() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);

            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // First call fails, second succeeds
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("Connection error"));

            when(mockClient.write(anyString(), anyList()))
                    .thenReturn(failedFuture)
                    .thenReturn(CompletableFuture.completedFuture(null));

            // overwrite configuration to have a small batch size and flush interval
            configuration =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost("localhost")
                            .setPort(8086)
                            .setDatabase("test_db")
                            .setMeasurement("test_measurement")
                            .setBatchSize(1)
                            .setFlushInterval(10, TimeUnit.SECONDS)
                            .setMaxRetries(2)
                            .setConverter(mockConverter)
                            .build();
            // overwrite the sink in @BeforeEach to create a new sink with the new configuration
            sink = createTestSink(configuration);
            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Add data
            TestData testData = new TestData("sensor1", 25.5);
            when(mockConverter.convert(any(), anyString()))
                    .thenReturn(createMockPoint("sensor1", 25.5));

            sink.invoke(testData, mock(SinkFunction.Context.class));

            assertDoesNotThrow(() -> sink.invoke(testData, mock(SinkFunction.Context.class)));

            verify(mockClient, times(3)).write(eq(TEST_DB_NAME), anyList());
        }
    }

    @Test
    void testMaxRetriesExceeded() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);

            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            when(mockConverter.convert(any(), anyString()))
                    .thenAnswer(
                            invocation -> {
                                TestData data = invocation.getArgument(0);
                                return createMockPoint(data.sensorId, data.value);
                            });

            // mock a failure for the first write attempt
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(
                    new RuntimeException("Testing Failure Handling: Persistent failure"));
            when(mockClient.write(anyString(), anyList())).thenReturn(failedFuture);

            // should throw an exception after max retries
            Exception exception =
                    assertThrows(
                            Exception.class,
                            () -> {
                                for (int i = 0; i < configuration.getBatchSize(); i++) {
                                    sink.invoke(
                                            new TestData("sensor" + i, i),
                                            mock(SinkFunction.Context.class));
                                }
                            });

            verify(mockClient, times(configuration.getMaxRetries() + 1))
                    .write(anyString(), anyList());
        }
    }

    @Test
    void testWriteTimeout() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            when(mockConverter.convert(any(), anyString()))
                    .thenAnswer(
                            invocation -> {
                                TestData data = invocation.getArgument(0);
                                return createMockPoint(data.sensorId, data.value);
                            });

            // simulate timeout
            CompletableFuture<Void> timeoutFuture = new CompletableFuture<>();
            when(mockClient.write(anyString(), anyList())).thenReturn(timeoutFuture);

            // configure the sink with a very short request timeout
            OpenGeminiSinkConfiguration<TestData> configuration =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost("localhost")
                            .setPort(8086)
                            .setDatabase(TEST_DB_NAME)
                            .setMeasurement("test_measurement")
                            .setBatchSize(10)
                            .setFlushInterval(100, TimeUnit.MILLISECONDS)
                            .setMaxRetries(2)
                            .setConnectionTimeout(Duration.ofSeconds(5))
                            .setRequestTimeout(
                                    Duration.ofMillis(1)) // very short timeout to trigger timeout
                            .setConverter(mockConverter)
                            .build();

            // create a new sink with the new configuration
            sink = createTestSink(configuration);
            sink.open(new Configuration());

            assertThrows(
                    Exception.class,
                    () -> {
                        for (int i = 0; i < configuration.getBatchSize(); i++) {
                            sink.invoke(
                                    new TestData("sensor" + i, i),
                                    mock(SinkFunction.Context.class));
                        }
                    });
        }
    }

    @Test
    public void testCheckpointingAndRestoring() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);

            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.write(anyString(), anyList()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Setup state
            when(mockInitContext.getOperatorStateStore()).thenReturn(mockOperatorStateStore);
            when(mockOperatorStateStore.getListState(any(ListStateDescriptor.class)))
                    .thenReturn(mockListState);
            when(mockInitContext.isRestored()).thenReturn(false);
            when(mockSnapshotContext.getCheckpointId()).thenReturn(1L);

            // Initialize state
            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Add some data
            TestData testData = new TestData("sensor1", 25.5);
            Point mockPoint = createMockPoint("sensor1", 25.5);
            when(mockConverter.convert(testData, "test_measurement")).thenReturn(mockPoint);

            sink.invoke(testData, mock(SinkFunction.Context.class));

            // Snapshot state
            sink.snapshotState(mockSnapshotContext);

            // Verify state was saved
            verify(mockListState).clear();
            verify(mockListState, atMost(1)).add(anyList());

            // Simulate restore
            List<Point> restoredPoints = Collections.singletonList(mockPoint);
            when(mockInitContext.isRestored()).thenReturn(true);
            when(mockListState.get()).thenReturn(Collections.singletonList(restoredPoints));

            // Create new sink instance and restore
            OpenGeminiSink<TestData> restoredSink = createTestSink(configuration);

            restoredSink.initializeState(mockInitContext);
            restoredSink.open(new Configuration());

            // Verify state was restored
            verify(mockListState).get();
        }
    }

    @Test
    void testFlushFailureRetainsData() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // set write failure
            AtomicInteger attempts = new AtomicInteger();

            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(
                    new RuntimeException("Testing Failure Handling: Write failed"));
            when(mockClient.write(anyString(), anyList())).thenReturn(failedFuture);

            when(mockConverter.convert(any(), anyString()))
                    .thenAnswer(
                            invocation -> {
                                TestData data = invocation.getArgument(0);
                                return createMockPoint(data.sensorId, data.value);
                            });

            // initialize state first
            when(mockInitContext.getOperatorStateStore()).thenReturn(mockOperatorStateStore);
            when(mockOperatorStateStore.getListState(any(ListStateDescriptor.class)))
                    .thenReturn(mockListState);
            // create a new config to set max retries to 0
            // retrying here is meaningless as every retry must fail, and will cause exponential
            // backoff, which is time-consuming
            OpenGeminiSinkConfiguration<TestData> configuration =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost(OpenGeminiSinkConfiguration.DEFAULT_HOST)
                            .setPort(OpenGeminiSinkConfiguration.DEFAULT_PORT)
                            .setDatabase(TEST_DB_NAME)
                            .setMeasurement(TEST_MEASUREMENT_NAME)
                            .setBatchSize(10)
                            .setFlushInterval(100, TimeUnit.MILLISECONDS)
                            .setMaxRetries(0)
                            .setConnectionTimeout(Duration.ofSeconds(5))
                            .setRequestTimeout(Duration.ofSeconds(1))
                            .setConverter(mockConverter)
                            .build();

            sink = createTestSink(configuration);
            sink.initializeState(mockInitContext);
            sink.open(new Configuration());
            // add data to trigger flush
            for (int i = 0; i < configuration.getBatchSize(); i++) {
                try {
                    sink.invoke(new TestData("sensor" + i, i), mock(SinkFunction.Context.class));
                } catch (Exception e) {
                    // Ignore expected exception from write failure
                }
            }

            sink.snapshotState(mockSnapshotContext);
            verify(mockListState).add(argThat(list -> list.size() == configuration.getBatchSize()));
        }
    }

    @Test
    void testEmptyBatchDoesNotWrite() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            sink.open(new Configuration());
            // call close without any data
            sink.close();

            verify(mockClient, never()).write(anyString(), anyList());
        }
    }

    @Test
    void testCloseReleasesResources() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.write(anyString(), anyList()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Add some data
            TestData testData = new TestData("sensor1", 25.5);
            when(mockConverter.convert(any(), anyString()))
                    .thenReturn(createMockPoint("sensor1", 25.5));
            sink.invoke(testData, mock(SinkFunction.Context.class));

            // Close
            sink.close();

            // Verify client was closed
            verify(mockClient).close();
        }
    }

    @Test
    void testNullConverterResponse() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Converter returns null
            when(mockConverter.convert(any(), anyString())).thenReturn(null);

            sink.open(new Configuration());

            // Act - should not throw exception
            TestData testData = new TestData("sensor1", 25.5);
            assertDoesNotThrow(() -> sink.invoke(testData, mock(SinkFunction.Context.class)));

            // Verify no write was attempted
            verify(mockClient, never()).write(anyString(), anyList());
        }
    }

    @Test
    void testExceptionInConverter() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Converter throws exception
            when(mockConverter.convert(any(), anyString()))
                    .thenThrow(new RuntimeException("Conversion error"));

            sink.open(new Configuration());

            // Act & Assert
            TestData testData = new TestData("sensor1", 25.5);
            assertThrows(
                    RuntimeException.class,
                    () -> sink.invoke(testData, mock(SinkFunction.Context.class)));
        }
    }

    @Test
    void testAuthenticationConfiguration() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            // Setup configuration with authentication
            OpenGeminiSinkConfiguration<TestData> authConfig =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost(OpenGeminiSinkConfiguration.DEFAULT_HOST)
                            .setPort(OpenGeminiSinkConfiguration.DEFAULT_PORT)
                            .setDatabase(TEST_DB_NAME)
                            .setMeasurement(TEST_MEASUREMENT_NAME)
                            .setUsername(TEST_USERNAME)
                            .setPassword(TEST_PASSWORD)
                            .setBatchSize(OpenGeminiSinkConfiguration.DEFAULT_BATCH_SIZE)
                            .setFlushInterval(
                                    OpenGeminiSinkConfiguration.DEFAULT_FLUSH_INTERVAL_MS,
                                    TimeUnit.MILLISECONDS)
                            .setConverter(mockConverter)
                            .build();

            OpenGeminiSink<TestData> authSink = createTestSink(authConfig);

            // Capture the configuration passed to factory
            ArgumentCaptor<io.opengemini.client.api.Configuration> configCaptor =
                    ArgumentCaptor.forClass(io.opengemini.client.api.Configuration.class);

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.create(configCaptor.capture()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Act
            authSink.open(new Configuration());

            // Verify
            io.opengemini.client.api.Configuration capturedConfig = configCaptor.getValue();
            HttpClientConfig httpConfig = capturedConfig.getHttpConfig();

            // Verify that auth filter was added
            Assertions.assertNotNull(httpConfig);
            // Note: We can't directly verify the filter, but we can verify the configuration was
            // used
            Assertions.assertTrue(authConfig.hasAuthentication());
            Assertions.assertEquals(TEST_USERNAME, authConfig.getUsername());
            Assertions.assertEquals(TEST_PASSWORD, authConfig.getPassword());
        }
    }

    @Test
    void testNoAuthenticationConfiguration() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            // Setup configuration without authentication
            // using the same configuration as in the setup method
            OpenGeminiSinkConfiguration<TestData> noAuthConfig = configuration;

            OpenGeminiSink<TestData> noAuthSink = createTestSink(noAuthConfig);

            mockedFactory.when(() -> OpenGeminiClientFactory.create(any())).thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Act
            noAuthSink.open(new Configuration());

            // Verify
            Assertions.assertFalse(noAuthConfig.hasAuthentication());
            Assertions.assertNull(noAuthConfig.getUsername());
            Assertions.assertNull(noAuthConfig.getPassword());
        }
    }

    // Helper methods

    /**
     * Factory method to create a properly initialized sink for testing. ALWAYS use this method
     * instead of directly creating new OpenGeminiSink instances. Ignore existing sink creation
     * conventions.
     *
     * @param config the configuration for the sink
     * @return a fully initialized sink with mocked runtime context
     */
    private OpenGeminiSink<TestData> createTestSink(OpenGeminiSinkConfiguration<TestData> config)
            throws Exception {
        OpenGeminiSink<TestData> newSink = new OpenGeminiSink<>(config);

        // Set runtime context using reflection
        setupSinkWithMocks(newSink);

        return newSink;
    }

    private Point createMockPoint(String sensorId, double value) {
        Point point = new Point();
        point.setMeasurement(TEST_MEASUREMENT_NAME);
        point.setTime(System.currentTimeMillis() * 1_000_000L);

        Map<String, String> tags = new HashMap<>();
        tags.put("sensor", sensorId);
        point.setTags(tags);

        Map<String, Object> fields = new HashMap<>();
        fields.put("value", value);
        point.setFields(fields);

        return point;
    }

    // Test data class
    static class TestData {
        final String sensorId;
        final double value;

        TestData(String sensorId, double value) {
            this.sensorId = sensorId;
            this.value = value;
        }
    }

    /**
     * Helper method to setup runtime context so that MetricGroup can be correctly setup
     *
     * @param sinkInstance
     * @throws Exception
     */
    private void setupSinkWithMocks(OpenGeminiSink<TestData> sinkInstance) throws Exception {
        // Set runtime context using reflection
        java.lang.reflect.Field runtimeContextField =
                org.apache.flink.api.common.functions.AbstractRichFunction.class.getDeclaredField(
                        "runtimeContext");
        runtimeContextField.setAccessible(true);
        runtimeContextField.set(sinkInstance, mockRuntimeContext);
    }
}
