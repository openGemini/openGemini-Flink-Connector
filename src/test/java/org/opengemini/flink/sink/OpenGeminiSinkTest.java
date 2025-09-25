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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.opengemini.flink.utils.EnhancedOpenGeminiClient;
import org.opengemini.flink.utils.OpenGeminiClientFactory;

import io.github.openfacade.http.HttpClientConfig;
import io.opengemini.client.api.Point;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class OpenGeminiSinkTest {

    @Mock private EnhancedOpenGeminiClient mockClient;

    @Mock private OpenGeminiPointConverter<TestData> mockPointConverter;
    @Mock private OpenGeminiLineProtocolConverter<TestData> mockLineProtocolConverter;
    @Mock private ListState<List<String>> mockListState;

    @Mock private FunctionInitializationContext mockInitContext;

    @Mock private OperatorStateStore mockOperatorStateStore;
    @Mock private FunctionSnapshotContext mockSnapshotContext;

    @Mock private RuntimeContext mockRuntimeContext;
    @Mock private OperatorMetricGroup mockMetricGroup;
    @Mock private Counter mockCounter;
    // WARNING: if a new sink is to be created in an individual test, MUST use createTestSink helper
    // method
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
                        .setConverter(mockLineProtocolConverter)
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
        try (MockedStatic<org.opengemini.flink.utils.OpenGeminiClientFactory> mockedFactory =
                mockStatic(org.opengemini.flink.utils.OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            // Act
            sink.open(new Configuration());

            // Verify
            mockedFactory.verify(() -> OpenGeminiClientFactory.createEnhanced(any()), times(1));
            verify(mockClient).createDatabase(TEST_DB_NAME);
        }
    }

    @Test
    void testInvokeWithNullValue() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            sink.open(new Configuration());

            // Act - should not throw exception
            assertDoesNotThrow(() -> sink.invoke(null, mock(SinkFunction.Context.class)));

            // Verify converter was not called
            verify(mockLineProtocolConverter, never()).convertToLineProtocol(any(), anyString());
        }
    }

    @Test
    void testBatchingTriggersWhenBatchSizeReached() throws Exception {

        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);

            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.writeLineProtocols(anyString(), anyList()))
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
                            .setConverter(mockLineProtocolConverter)
                            .build();

            sink = createTestSink(configuration);
            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Create test data
            List<TestData> testDataList =
                    Arrays.asList(new TestData("sensor1", 25.5), new TestData("sensor2", 26.0));

            // Setup converter
            when(mockLineProtocolConverter.convertToLineProtocol(any(), anyString()))
                    .thenAnswer(
                            invocation -> {
                                TestData data = invocation.getArgument(0);
                                return createMockLineProtocol(data.sensorId, data.value);
                            });

            // Act - invoke twice to trigger batch
            for (TestData data : testDataList) {
                sink.invoke(data, mock(SinkFunction.Context.class));
            }

            // Verify batch was written
            verify(mockClient, atLeastOnce()).writeLineProtocols(eq(TEST_DB_NAME), anyList());
        }
    }

    @Test
    void testFlushIntervalTrigger() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.writeLineProtocols(anyString(), anyList()))
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
                            .setConverter(mockLineProtocolConverter)
                            .build();

            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Add one point
            TestData testData = new TestData("sensor1", 25.5);
            String mockLineProtocol = createMockLineProtocol("sensor1", 25.5);
            when(mockLineProtocolConverter.convertToLineProtocol(testData, TEST_MEASUREMENT_NAME))
                    .thenReturn(mockLineProtocol);

            sink.invoke(testData, mock(SinkFunction.Context.class));

            // Wait for flush interval
            Thread.sleep(configuration.getFlushIntervalMillis() + 50);

            // call invoke again to trigger flushing
            sink.invoke(testData, mock(SinkFunction.Context.class));

            // Verify flush occurred
            verify(mockClient, atLeastOnce()).writeLineProtocols(eq(TEST_DB_NAME), anyList());
        }
    }

    @Test
    void testRetryMechanism() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);

            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // First call fails, second succeeds
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("Connection error"));

            when(mockClient.writeLineProtocols(anyString(), anyList()))
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
                            .setConverter(mockLineProtocolConverter)
                            .build();
            // overwrite the sink in @BeforeEach to create a new sink with the new configuration
            sink = createTestSink(configuration);
            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Add data
            TestData testData = new TestData("sensor1", 25.5);
            when(mockLineProtocolConverter.convertToLineProtocol(any(), anyString()))
                    .thenReturn(createMockLineProtocol("sensor1", 25.5));

            sink.invoke(testData, mock(SinkFunction.Context.class));

            assertDoesNotThrow(() -> sink.invoke(testData, mock(SinkFunction.Context.class)));

            verify(mockClient, times(3)).writeLineProtocols(eq(TEST_DB_NAME), anyList());
        }
    }

    @Test
    void testMaxRetriesExceeded() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);

            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            when(mockLineProtocolConverter.convertToLineProtocol(any(), anyString()))
                    .thenAnswer(
                            invocation -> {
                                TestData data = invocation.getArgument(0);
                                return createMockLineProtocol(data.sensorId, data.value);
                            });

            // mock a failure for the first write attempt
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(
                    new RuntimeException("Testing Failure Handling: Persistent failure"));
            when(mockClient.writeLineProtocols(anyString(), anyList())).thenReturn(failedFuture);

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
                    .writeLineProtocols(anyString(), anyList());
        }
    }

    @Test
    void testWriteTimeout() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            when(mockLineProtocolConverter.convertToLineProtocol(any(), anyString()))
                    .thenAnswer(
                            invocation -> {
                                TestData data = invocation.getArgument(0);
                                return createMockLineProtocol(data.sensorId, data.value);
                            });

            // simulate timeout
            CompletableFuture<Void> timeoutFuture = new CompletableFuture<>();
            when(mockClient.writeLineProtocols(anyString(), anyList())).thenReturn(timeoutFuture);

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
                            .setConverter(mockLineProtocolConverter)
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
        try (MockedStatic<org.opengemini.flink.utils.OpenGeminiClientFactory> mockedFactory =
                mockStatic(org.opengemini.flink.utils.OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory
                    .when(
                            () ->
                                    org.opengemini.flink.utils.OpenGeminiClientFactory
                                            .createEnhanced(any()))
                    .thenReturn(mockClient);

            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.writeLineProtocols(anyString(), anyList()))
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
            String mockLineProtocol = createMockLineProtocol("sensor1", 25.5);
            when(mockLineProtocolConverter.convertToLineProtocol(testData, "test_measurement"))
                    .thenReturn(mockLineProtocol);

            sink.invoke(testData, mock(SinkFunction.Context.class));

            // Snapshot state
            sink.snapshotState(mockSnapshotContext);

            // Verify state was saved
            verify(mockListState).clear();
            verify(mockListState, atMost(1)).add(anyList());

            // Simulate restore
            List<String> restoredLineProtocols = Collections.singletonList(mockLineProtocol);
            when(mockInitContext.isRestored()).thenReturn(true);
            when(mockListState.get()).thenReturn(Collections.singletonList(restoredLineProtocols));

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
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // set write failure
            AtomicInteger attempts = new AtomicInteger();

            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(
                    new RuntimeException("Testing Failure Handling: Write failed"));
            when(mockClient.writeLineProtocols(anyString(), anyList())).thenReturn(failedFuture);

            when(mockLineProtocolConverter.convertToLineProtocol(any(), anyString()))
                    .thenAnswer(
                            invocation -> {
                                TestData data = invocation.getArgument(0);
                                return createMockLineProtocol(data.sensorId, data.value);
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
                            .setConverter(mockLineProtocolConverter)
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
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            sink.open(new Configuration());
            // call close without any data
            sink.close();

            verify(mockClient, never()).writeLineProtocols(anyString(), anyList());
        }
    }

    @Test
    void testCloseReleasesResources() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.writeLineProtocols(anyString(), anyList()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Add some data
            TestData testData = new TestData("sensor1", 25.5);
            when(mockLineProtocolConverter.convertToLineProtocol(any(), anyString()))
                    .thenReturn(createMockLineProtocol("sensor1", 25.5));
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
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Converter returns null
            when(mockLineProtocolConverter.convertToLineProtocol(any(), anyString()))
                    .thenReturn(null);

            sink.open(new Configuration());

            // Act - should not throw exception
            TestData testData = new TestData("sensor1", 25.5);
            assertDoesNotThrow(() -> sink.invoke(testData, mock(SinkFunction.Context.class)));

            // Verify no write was attempted
            verify(mockClient, never()).writeLineProtocols(anyString(), anyList());
        }
    }

    @Test
    void testExceptionInConverter() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {
            // Setup
            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Converter throws exception
            when(mockLineProtocolConverter.convertToLineProtocol(any(), anyString()))
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
                            .setConverter(mockLineProtocolConverter)
                            .build();

            OpenGeminiSink<TestData> authSink = createTestSink(authConfig);

            // Capture the configuration passed to factory
            ArgumentCaptor<io.opengemini.client.api.Configuration> configCaptor =
                    ArgumentCaptor.forClass(io.opengemini.client.api.Configuration.class);

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(configCaptor.capture()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Act
            authSink.open(new Configuration());

            // Verify
            io.opengemini.client.api.Configuration capturedConfig = configCaptor.getValue();
            HttpClientConfig httpConfig = capturedConfig.getHttpConfig();

            // Verify that auth filter was added
            assertNotNull(httpConfig);
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

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
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

    @Test
    void testLineProtocolConverterPath() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.writeLineProtocols(anyString(), anyList()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            configuration =
                    configuration.toBuilder().setConverter(mockLineProtocolConverter).build();

            sink = createTestSink(configuration);
            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            TestData testData = new TestData("sensor1", 25.5);
            String expectedLineProtocol = createMockLineProtocol("sensor1", 25.5);
            when(mockLineProtocolConverter.convertToLineProtocol(testData, TEST_MEASUREMENT_NAME))
                    .thenReturn(expectedLineProtocol);

            sink.invoke(testData, mock(SinkFunction.Context.class));

            verify(mockLineProtocolConverter)
                    .convertToLineProtocol(testData, TEST_MEASUREMENT_NAME);
            verify(mockPointConverter, never()).convertToPoint(any(), any());
        }
    }

    @Test
    void testNullLineProtocolHandling() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);

            when(mockLineProtocolConverter.convertToLineProtocol(any(), anyString()))
                    .thenReturn(null);

            sink = createTestSink(configuration);
            sink.open(new Configuration());

            TestData testData = new TestData("sensor1", 25.5);
            assertDoesNotThrow(() -> sink.invoke(testData, mock(SinkFunction.Context.class)));

            verify(mockClient, never()).writeLineProtocols(any(), any());
        }
    }

    @Test
    void testConstructor_WithLineProtocolConverter() {
        // Test constructor with OpenGeminiLineProtocolConverter
        OpenGeminiSinkConfiguration<TestData> config =
                OpenGeminiSinkConfiguration.<TestData>builder()
                        .setHost("localhost")
                        .setPort(8086)
                        .setDatabase(TEST_DB_NAME)
                        .setMeasurement(TEST_MEASUREMENT_NAME)
                        .setConverter(mockLineProtocolConverter)
                        .build();

        OpenGeminiSink<TestData> sink = new OpenGeminiSink<>(config);

        // Verify lineProtocolConverter is set and pointConverter is null
        // Note: We can't directly access these fields, but we can verify behavior
        assertNotNull(sink);
        assertEquals(config, getField(sink, "configuration"));
    }

    @Test
    void testConstructor_WithPointConverter() {
        // Test constructor with OpenGeminiPointConverter
        OpenGeminiSinkConfiguration<TestData> config =
                OpenGeminiSinkConfiguration.<TestData>builder()
                        .setHost("localhost")
                        .setPort(8086)
                        .setDatabase(TEST_DB_NAME)
                        .setMeasurement(TEST_MEASUREMENT_NAME)
                        .setConverter(mockPointConverter)
                        .build();

        OpenGeminiSink<TestData> sink = new OpenGeminiSink<>(config);

        assertNotNull(sink);
        assertEquals(config, getField(sink, "configuration"));
    }

    @Test
    void testConstructor_WithInvalidConverter() {
        // Test constructor with invalid converter type
        Object invalidConverter = new Object(); // Not a valid converter type

        OpenGeminiSinkConfiguration<TestData> config =
                OpenGeminiSinkConfiguration.<TestData>builder()
                        .setHost("localhost")
                        .setPort(8086)
                        .setDatabase(TEST_DB_NAME)
                        .setMeasurement(TEST_MEASUREMENT_NAME)
                        .setConverter(invalidConverter)
                        .build();

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new OpenGeminiSink<>(config));

        assertEquals(
                "Converter must be either OpenGeminiLineProtocolConverter or OpenGeminiPointConverter",
                exception.getMessage());
    }

    @Test
    void testOpen_WithPointConverter_SetsDirectConversionFalse() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            // Setup configuration with point converter
            OpenGeminiSinkConfiguration<TestData> config =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost("localhost")
                            .setPort(8086)
                            .setDatabase(TEST_DB_NAME)
                            .setMeasurement(TEST_MEASUREMENT_NAME)
                            .setConverter(mockPointConverter)
                            .build();

            OpenGeminiSink<TestData> sink = new OpenGeminiSink<>(config);
            setupSinkWithMocks(sink);

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Act
            sink.open(new Configuration());

            // Verify useDirectConversion is set to false (we can't directly access it,
            // but we can verify the log message or behavior)
            // The method should complete without throwing IllegalStateException
            assertDoesNotThrow(() -> sink.open(new Configuration()));
        }
    }

    @Test
    void testOpen_WithNoConverter_ThrowsIllegalStateException() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            // Create a sink with configuration that has no converter
            // This requires creating a special configuration or using reflection to set converter
            // to null
            OpenGeminiSinkConfiguration<TestData> config =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost("localhost")
                            .setPort(8086)
                            .setDatabase(TEST_DB_NAME)
                            .setMeasurement(TEST_MEASUREMENT_NAME)
                            .setConverter(mockLineProtocolConverter)
                            .build();

            OpenGeminiSink<TestData> sink = new OpenGeminiSink<>(config);
            setupSinkWithMocks(sink);

            // Use reflection to set both converters to null to simulate the error condition
            setField(sink, "lineProtocolConverter", null);
            setField(sink, "pointConverter", null);

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Act & Assert
            IllegalStateException exception =
                    assertThrows(IllegalStateException.class, () -> sink.open(new Configuration()));

            assertEquals(
                    "No converter configured. Either lineProtocolConverter or pointConverter must be set",
                    exception.getMessage());
        }
    }

    @Test
    void testInvoke_WithPointConverter_NullPointHandling() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            // Setup configuration with point converter
            OpenGeminiSinkConfiguration<TestData> config =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost("localhost")
                            .setPort(8086)
                            .setDatabase(TEST_DB_NAME)
                            .setMeasurement(TEST_MEASUREMENT_NAME)
                            .setConverter(mockPointConverter)
                            .build();

            OpenGeminiSink<TestData> sink = new OpenGeminiSink<>(config);
            setupSinkWithMocks(sink);

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Setup point converter to return null
            when(mockPointConverter.convertToPoint(any(), anyString())).thenReturn(null);

            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            TestData testData = new TestData("sensor1", 25.5);

            // Act - should not throw exception and should return early
            assertDoesNotThrow(() -> sink.invoke(testData, mock(SinkFunction.Context.class)));

            // Verify converter was called but no write occurred
            verify(mockPointConverter).convertToPoint(testData, TEST_MEASUREMENT_NAME);
            verify(mockClient, never()).writeLineProtocols(anyString(), anyList());
        }
    }

    @Test
    void testInvoke_WithPointConverter_ValidPoint() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            // Setup configuration with point converter
            OpenGeminiSinkConfiguration<TestData> config =
                    OpenGeminiSinkConfiguration.<TestData>builder()
                            .setHost("localhost")
                            .setPort(8086)
                            .setDatabase(TEST_DB_NAME)
                            .setMeasurement(TEST_MEASUREMENT_NAME)
                            .setBatchSize(1) // Small batch size to trigger immediate write
                            .setConverter(mockPointConverter)
                            .build();

            OpenGeminiSink<TestData> sink = new OpenGeminiSink<>(config);
            setupSinkWithMocks(sink);

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(mockClient.writeLineProtocols(anyString(), anyList()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Mock Point object
            Point mockPoint = mock(Point.class);
            String expectedLineProtocol = createMockLineProtocol("sensor1", 25.5);
            when(mockPoint.lineProtocol()).thenReturn(expectedLineProtocol);
            when(mockPointConverter.convertToPoint(any(), anyString())).thenReturn(mockPoint);

            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            TestData testData = new TestData("sensor1", 25.5);

            // Act
            sink.invoke(testData, mock(SinkFunction.Context.class));

            // Verify the flow: converter called, point.lineProtocol() called, write occurred
            verify(mockPointConverter).convertToPoint(testData, TEST_MEASUREMENT_NAME);
            verify(mockPoint).lineProtocol();
            verify(mockClient).writeLineProtocols(eq(TEST_DB_NAME), anyList());
        }
    }

    @Test
    void testClose_ClientCloseThrowsException() throws Exception {
        try (MockedStatic<OpenGeminiClientFactory> mockedFactory =
                mockStatic(OpenGeminiClientFactory.class)) {

            mockedFactory
                    .when(() -> OpenGeminiClientFactory.createEnhanced(any()))
                    .thenReturn(mockClient);
            when(mockClient.createDatabase(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            // Setup client.close() to throw an exception
            RuntimeException closeException = new RuntimeException("Connection lost");
            doThrow(closeException).when(mockClient).close();

            sink.initializeState(mockInitContext);
            sink.open(new Configuration());

            // Act & Assert
            RuntimeException exception = assertThrows(RuntimeException.class, () -> sink.close());

            assertEquals("Failed to close OpenGemini client", exception.getMessage());
            assertEquals(closeException, exception.getCause());

            // Verify close was attempted
            verify(mockClient).close();
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
        OpenGeminiSink<TestData> newSink = new OpenGeminiSink<>(config, mockLineProtocolConverter);

        // Set runtime context using reflection
        setupSinkWithMocks(newSink);

        return newSink;
    }

    private String createMockLineProtocol(String sensorId, double value) {
        return String.format(
                "%s,sensor=%s value=%f %d",
                TEST_MEASUREMENT_NAME, sensorId, value, System.nanoTime());
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

    // Helper methods for reflection access
    @SuppressWarnings("unchecked")
    private <T> T getField(Object target, String fieldName) {
        try {
            java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(target);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get field: " + fieldName, e);
        }
    }

    private void setField(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field: " + fieldName, e);
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
