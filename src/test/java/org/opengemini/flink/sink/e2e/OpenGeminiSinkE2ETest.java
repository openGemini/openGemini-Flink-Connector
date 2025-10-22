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
package org.opengemini.flink.sink.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.function.SerializableFunction;
import org.junit.jupiter.api.*;
import org.opengemini.flink.sink.*;
import org.opengemini.flink.sink.e2e.model.SensorData;
import org.opengemini.flink.sink.e2e.utils.SensorDataGenerator;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.github.openfacade.http.HttpClientConfig;
import io.opengemini.client.api.Query;
import io.opengemini.client.api.QueryResult;
import io.opengemini.client.api.Series;
import io.opengemini.client.impl.OpenGeminiClient;
import io.opengemini.client.impl.OpenGeminiClientFactory;

import lombok.extern.slf4j.Slf4j;

/** End-to-end tests for OpenGeminiSink. */
@Slf4j
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OpenGeminiSinkE2ETest {

    private static final String DOCKER_IMAGE = "opengeminidb/opengemini-server:latest";
    private static final int OPENGEMINI_PORT = 8086;
    private static final String TEST_DATABASE = "testdb";
    private static final String TEST_MEASUREMENT = "sensor_data";

    @Container
    private static final GenericContainer<?> openGeminiContainer =
            new GenericContainer<>(DockerImageName.parse(DOCKER_IMAGE))
                    .withExposedPorts(OPENGEMINI_PORT)
                    .waitingFor(Wait.forHttp("/ping").forPort(OPENGEMINI_PORT).forStatusCode(204))
                    .withStartupTimeout(Duration.ofMinutes(2));

    private static OpenGeminiClient client;
    private static String openGeminiUrl;

    @BeforeAll
    static void setupContainer() throws Exception {
        // Get container connection details
        String host = openGeminiContainer.getHost();
        Integer port = openGeminiContainer.getMappedPort(OPENGEMINI_PORT);
        openGeminiUrl = String.format("http://%s:%d", host, port);

        log.info("OpenGemini container started at: {}", openGeminiUrl);

        HttpClientConfig httpConfig =
                new HttpClientConfig.Builder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .timeout(Duration.ofSeconds(30))
                        .build();

        // Initialize client
        client =
                OpenGeminiClientFactory.create(
                        io.opengemini.client.api.Configuration.builder()
                                .addresses(
                                        Arrays.asList(
                                                new io.opengemini.client.api.Address(host, port)))
                                .httpConfig(httpConfig)
                                .build());

        // Create test database
        client.createDatabase(TEST_DATABASE).get(10, TimeUnit.SECONDS);
        log.info("Created test database: {}", TEST_DATABASE);
    }

    @AfterAll
    static void teardownContainer() throws Exception {
        if (client != null) {
            client.dropDatabase(TEST_DATABASE).get(10, TimeUnit.SECONDS);
            client.close();
        }
    }

    @BeforeEach
    void clearMeasurement() throws Exception {
        log.info("=== Clearing measurement before test ===");

        // ensures that database exists
        try {
            client.createDatabase(TEST_DATABASE).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            // ignore potential exception caused by existing database
        }

        // delete the whole database
        try {
            client.dropDatabase(TEST_DATABASE).get(5, TimeUnit.SECONDS);
            log.info("Dropped database: {}", TEST_DATABASE);
            Thread.sleep(500);

            client.createDatabase(TEST_DATABASE).get(5, TimeUnit.SECONDS);
            log.info("Recreated database: {}", TEST_DATABASE);
        } catch (Exception e) {
            log.error("Error clearing database", e);
            // if failed, use the alternative method
            clearMeasurementAlternative();
        }
    }

    private void clearMeasurementAlternative() throws Exception {
        // alternative method: delete all data with delete query
        try {
            String deleteQuery = String.format("DELETE FROM %s WHERE time > 0", TEST_MEASUREMENT);
            Query query = new Query(deleteQuery);
            query.setDatabase(TEST_DATABASE);
            client.query(query).get(5, TimeUnit.SECONDS);
            log.info("Deleted all data from measurement: {}", TEST_MEASUREMENT);
        } catch (Exception e) {
            log.debug("Measurement might not exist: {}", e.getMessage());
        }
    }

    @Test
    @DisplayName("Test basic data flow from Flink to OpenGemini")
    void testBasicDataFlow() throws Exception {
        // Configure converter
        OpenGeminiPointConverter<SensorData> converter =
                SimpleOpenGeminiPointConverter.<SensorData>builder()
                        .addTag(
                                "sensor_id",
                                (SerializableFunction<SensorData, String>) SensorData::getSensorId)
                        .addTag(
                                "location",
                                (SerializableFunction<SensorData, String>) SensorData::getLocation)
                        .addField(
                                "temperature",
                                (SerializableFunction<SensorData, java.lang.Object>)
                                        SensorData::getTemperature)
                        .addField(
                                "humidity",
                                (SerializableFunction<SensorData, java.lang.Object>)
                                        SensorData::getHumidity)
                        .addField(
                                "pressure",
                                (SerializableFunction<SensorData, java.lang.Object>)
                                        SensorData::getPressure)
                        .withTimestampMillis(
                                (SerializableFunction<SensorData, Long>) SensorData::getTimestamp)
                        .build();

        // Configure sink
        OpenGeminiSinkConfiguration<SensorData> config =
                OpenGeminiSinkConfiguration.<SensorData>builder()
                        .setUrl(openGeminiUrl)
                        .setDatabase(TEST_DATABASE)
                        .setMeasurement(TEST_MEASUREMENT)
                        .setConverter(converter)
                        .setBatchSize(10)
                        .setFlushInterval(100, TimeUnit.MILLISECONDS)
                        .build();

        // Setup Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long numberOfRecords = 100;
        env.addSource(new SensorDataGenerator(numberOfRecords, 10))
                .addSink(new OpenGeminiSink<>(config));

        // synchronous execution to ensure job completion before verification
        env.execute("Basic E2E Test");

        // verify result
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(
                        () -> {
                            long count = countRecords();
                            log.info("Current record count: {}", count);
                            assertThat(count).isEqualTo(numberOfRecords);
                        });

        verifyDataIntegrity();
    }

    @Test
    @DisplayName("Test multiple data types")
    void testMultipleDataTypes() throws Exception {
        // Test with different field types
        OpenGeminiPointConverter<ComplexTestData> converter =
                SimpleOpenGeminiPointConverter.<ComplexTestData>builder()
                        .addTag("id", ComplexTestData::getId)
                        .addTag("category", ComplexTestData::getCategory)
                        .addField("intValue", ComplexTestData::getIntValue)
                        .addField("longValue", ComplexTestData::getLongValue)
                        .addField("floatValue", ComplexTestData::getFloatValue)
                        .addField("doubleValue", ComplexTestData::getDoubleValue)
                        .addField("boolValue", ComplexTestData::isBoolValue)
                        .addField("stringValue", ComplexTestData::getStringValue)
                        .withTimestampMillis(ComplexTestData::getTimestamp)
                        .build();

        OpenGeminiSinkConfiguration<ComplexTestData> config =
                OpenGeminiSinkConfiguration.<ComplexTestData>builder()
                        .setUrl(openGeminiUrl)
                        .setDatabase(TEST_DATABASE)
                        .setMeasurement("complex_data")
                        .setConverter(converter)
                        .setBatchSize(100)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Generate test data
        env.addSource(new ComplexDataGenerator(50)).addSink(new OpenGeminiSink<>(config));

        env.execute("Complex Data Type Test");

        // Verify data
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(
                        () -> {
                            String query = "SELECT * FROM complex_data LIMIT 1";
                            Query resultQuery = new Query(query);
                            resultQuery.setDatabase(TEST_DATABASE);
                            QueryResult result = client.query(resultQuery).get();

                            assertThat(result.getResults()).isNotEmpty();

                            // Verify all field types are present
                            List<Series> series = result.getResults().get(0).getSeries();
                            assertThat(series).isNotEmpty();

                            List<String> columns = series.get(0).getColumns();
                            assertThat(columns)
                                    .contains(
                                            "intValue",
                                            "longValue",
                                            "floatValue",
                                            "doubleValue",
                                            "boolValue",
                                            "stringValue");
                        });
    }

    @Test
    @DisplayName("Test Line Protocol converter for high performance")
    void testLineProtocolConverter() throws Exception {
        OpenGeminiLineProtocolConverter<SensorData> lineProtocolConverter =
                new OpenGeminiLineProtocolConverter<SensorData>() {
                    @Override
                    public String convertToLineProtocol(SensorData data, String measurement) {
                        return String.format(
                                "%s,sensor_id=%s,location=%s temperature=%f,humidity=%f,pressure=%f %d",
                                measurement,
                                data.getSensorId(),
                                data.getLocation().replace(" ", "\\ "),
                                data.getTemperature(),
                                data.getHumidity(),
                                data.getPressure(),
                                data.getTimestamp() * 1_000_000L);
                    }
                };

        OpenGeminiSinkConfiguration<SensorData> config =
                OpenGeminiSinkConfiguration.<SensorData>builder()
                        .setUrl(openGeminiUrl)
                        .setDatabase(TEST_DATABASE)
                        .setMeasurement(TEST_MEASUREMENT)
                        .setBatchSize(10)
                        .setFlushInterval(100, TimeUnit.MILLISECONDS)
                        .build();

        OpenGeminiSink<SensorData> sink = new OpenGeminiSink<>(config, lineProtocolConverter);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long numberOfRecords = 100;
        env.addSource(new SensorDataGenerator(numberOfRecords, 10)).addSink(sink);

        // synchronous execution to ensure job completion before verification
        log.info("=== Starting Flink job synchronously ===");
        env.execute("Line Protocol E2E Test");
        log.info("=== Flink job completed successfully ===");

        // verify result
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(
                        () -> {
                            long count = countRecords();
                            log.info("Current record count (Line Protocol): {}", count);
                            assertThat(count).isEqualTo(numberOfRecords);
                        });

        verifyDataIntegrity();
        log.info("Line Protocol converter test passed successfully");
    }

    @Test
    @DisplayName("Test SimpleLineProtocolConverter builder pattern")
    void testSimpleLineProtocolConverter() throws Exception {
        OpenGeminiLineProtocolConverter<SensorData> converter =
                SimpleOpenGeminiLineProtocolConverter.<SensorData>builder()
                        .addTag("sensor_id", SensorData::getSensorId)
                        .addTag("location", SensorData::getLocation)
                        .addField("temperature", data -> data.getTemperature())
                        .addField("humidity", data -> data.getHumidity())
                        .addField("pressure", data -> data.getPressure())
                        .withTimestampMillis(SensorData::getTimestamp)
                        .build();

        OpenGeminiSinkConfiguration<SensorData> config =
                OpenGeminiSinkConfiguration.<SensorData>builder()
                        .setUrl(openGeminiUrl)
                        .setDatabase(TEST_DATABASE)
                        .setMeasurement("sensor_data_lp")
                        .setBatchSize(20)
                        .setFlushInterval(200, TimeUnit.MILLISECONDS)
                        .build();

        OpenGeminiSink<SensorData> sink = new OpenGeminiSink<>(config, converter);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long numberOfRecords = 50;
        env.addSource(new SensorDataGenerator(numberOfRecords, 10)).addSink(sink);

        env.execute("Simple Line Protocol Converter Test");

        // Verify
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(
                        () -> {
                            String countQuery = "SELECT COUNT(*) FROM sensor_data_lp";
                            Query query = new Query(countQuery);
                            query.setDatabase(TEST_DATABASE);
                            QueryResult result = client.query(query).get(5, TimeUnit.SECONDS);

                            long count = 0;
                            if (!result.getResults().isEmpty()
                                    && result.getResults().get(0).getSeries() != null
                                    && !result.getResults().get(0).getSeries().isEmpty()) {
                                List<List<Object>> values =
                                        result.getResults().get(0).getSeries().get(0).getValues();
                                if (!values.isEmpty() && !values.get(0).isEmpty()) {
                                    count = ((Number) values.get(0).get(1)).longValue();
                                }
                            }

                            assertThat(count).isEqualTo(numberOfRecords);
                        });
    }

    @Test
    @DisplayName("Test Line Protocol with complex data types")
    void testLineProtocolWithComplexTypes() throws Exception {
        OpenGeminiLineProtocolConverter<ComplexTestData> converter =
                new OpenGeminiLineProtocolConverter<ComplexTestData>() {
                    @Override
                    public String convertToLineProtocol(ComplexTestData data, String measurement) {
                        return String.format(
                                "%s,id=%s,category=%s intValue=%di,longValue=%di,floatValue=%f,doubleValue=%f,boolValue=%s,stringValue=\"%s\" %d",
                                measurement,
                                data.getId(),
                                data.getCategory(),
                                data.getIntValue(),
                                data.getLongValue(),
                                data.getFloatValue(),
                                data.getDoubleValue(),
                                data.isBoolValue(),
                                data.getStringValue().replace("\"", "\\\""),
                                data.getTimestamp() * 1_000_000L);
                    }
                };

        OpenGeminiSinkConfiguration<ComplexTestData> config =
                OpenGeminiSinkConfiguration.<ComplexTestData>builder()
                        .setUrl(openGeminiUrl)
                        .setDatabase(TEST_DATABASE)
                        .setMeasurement("complex_data_lp")
                        .setBatchSize(50)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ComplexDataGenerator(30))
                .addSink(new OpenGeminiSink<>(config, converter));

        env.execute("Complex Line Protocol Test");

        // Verify all data types are correctly stored
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(
                        () -> {
                            String query = "SELECT * FROM complex_data_lp LIMIT 1";
                            Query resultQuery = new Query(query);
                            resultQuery.setDatabase(TEST_DATABASE);
                            QueryResult result = client.query(resultQuery).get();

                            assertThat(result.getResults()).isNotEmpty();
                            List<Series> series = result.getResults().get(0).getSeries();
                            assertThat(series).isNotEmpty();

                            // verify that all expected columns are present
                            List<String> columns = series.get(0).getColumns();
                            assertThat(columns)
                                    .containsAll(
                                            Arrays.asList(
                                                    "intValue",
                                                    "longValue",
                                                    "floatValue",
                                                    "doubleValue",
                                                    "boolValue",
                                                    "stringValue"));
                        });
    }

    // Helper methods

    private long countRecords() throws Exception {
        String countQuery = String.format("SELECT COUNT(*) FROM %s", TEST_MEASUREMENT);
        log.info("=== COUNT: Executing query: {}", countQuery);
        Query query = new Query(countQuery);
        query.setDatabase(TEST_DATABASE);
        QueryResult result = client.query(query).get(5, TimeUnit.SECONDS);

        log.info("=== COUNT: Query result: {}", result);
        if (result.getResults().isEmpty()
                || result.getResults().get(0).getSeries() == null
                || result.getResults().get(0).getSeries().isEmpty()) {
            return 0;
        }

        List<List<java.lang.Object>> values =
                result.getResults().get(0).getSeries().get(0).getValues();
        if (values.isEmpty() || values.get(0).isEmpty()) {
            return 0;
        }

        return ((Number) values.get(0).get(1)).longValue();
    }

    private void verifyDataIntegrity() throws Exception {
        String query = String.format("SELECT * FROM %s LIMIT 10", TEST_MEASUREMENT);

        Query verificationQuery = new Query(query);
        verificationQuery.setDatabase(TEST_DATABASE);

        QueryResult result = client.query(verificationQuery).get(5, TimeUnit.SECONDS);
        log.debug("Query result: {}", result);
        assertThat(result.getResults()).isNotEmpty();

        if (result.getResults().get(0).getError() != null) {
            throw new AssertionError("Query error: " + result.getResults().get(0).getError());
        }

        List<Series> series = result.getResults().get(0).getSeries();

        assertThat(series).isNotEmpty();
    }

    // Additional test data models

    static class ComplexTestData {
        private final String id;
        private final String category;
        private final int intValue;
        private final long longValue;
        private final float floatValue;
        private final double doubleValue;
        private final boolean boolValue;
        private final String stringValue;
        private final long timestamp;

        public ComplexTestData(
                String id,
                String category,
                int intValue,
                long longValue,
                float floatValue,
                double doubleValue,
                boolean boolValue,
                String stringValue,
                long timestamp) {
            this.id = id;
            this.category = category;
            this.intValue = intValue;
            this.longValue = longValue;
            this.floatValue = floatValue;
            this.doubleValue = doubleValue;
            this.boolValue = boolValue;
            this.stringValue = stringValue;
            this.timestamp = timestamp;
        }

        // Getters
        public String getId() {
            return id;
        }

        public String getCategory() {
            return category;
        }

        public int getIntValue() {
            return intValue;
        }

        public long getLongValue() {
            return longValue;
        }

        public float getFloatValue() {
            return floatValue;
        }

        public double getDoubleValue() {
            return doubleValue;
        }

        public boolean isBoolValue() {
            return boolValue;
        }

        public String getStringValue() {
            return stringValue;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    static class ComplexDataGenerator implements SourceFunction<ComplexTestData> {
        private final long numberOfRecords;
        private volatile boolean running = true;

        public ComplexDataGenerator(long numberOfRecords) {
            this.numberOfRecords = numberOfRecords;
        }

        @Override
        public void run(SourceContext<ComplexTestData> ctx) throws Exception {
            for (int i = 0; i < numberOfRecords && running; i++) {
                ComplexTestData data =
                        new ComplexTestData(
                                "id-" + i,
                                "category-" + (i % 5),
                                i,
                                i * 1000L,
                                i * 0.1f,
                                i * 0.01,
                                i % 2 == 0,
                                "value-" + i,
                                System.currentTimeMillis());
                ctx.collect(data);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
