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
package org.opengemini.flink.table.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.*;
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

/** End-to-end tests for OpenGemini Table API connector. */
@Slf4j
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OpenGeminiTableE2ETest {

    private static final String DOCKER_IMAGE = "opengeminidb/opengemini-server:latest";
    private static final int OPENGEMINI_PORT = 8086;
    private static final String TEST_DATABASE = "tabledb";

    @Container
    private static final GenericContainer<?> openGeminiContainer =
            new GenericContainer<>(DockerImageName.parse(DOCKER_IMAGE))
                    .withExposedPorts(OPENGEMINI_PORT)
                    .waitingFor(Wait.forHttp("/ping").forPort(OPENGEMINI_PORT).forStatusCode(204))
                    .withStartupTimeout(Duration.ofMinutes(2));

    private static OpenGeminiClient client;
    private static String host;
    private static Integer port;

    private StreamTableEnvironment tableEnv;

    @BeforeAll
    static void setupContainer() throws Exception {
        // Get container connection details
        host = openGeminiContainer.getHost();
        port = openGeminiContainer.getMappedPort(OPENGEMINI_PORT);
        String openGeminiUrl = String.format("http://%s:%d", host, port);

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
    void setup() {
        // Setup Flink Table Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tableEnv = StreamTableEnvironment.create(env);

        // Clear database
        clearDatabase();
    }

    private void clearDatabase() {
        try {
            client.dropDatabase(TEST_DATABASE).get(5, TimeUnit.SECONDS);
            Thread.sleep(500);
            client.createDatabase(TEST_DATABASE).get(5, TimeUnit.SECONDS);
            log.info("Database cleared and recreated");
        } catch (Exception e) {
            log.error("Error clearing database", e);
        }
    }

    @Test
    @Order(1)
    @DisplayName("Test basic DDL and DML operations")
    void testBasicTableOperations() throws Exception {
        // Create sink table
        String createSinkTable =
                String.format(
                        "CREATE TABLE sensor_sink ("
                                + "  `timestamp` TIMESTAMP(3),"
                                + "  `sensor_id` STRING,"
                                + "  `location` STRING,"
                                + "  `temperature` DOUBLE,"
                                + "  `humidity` DOUBLE,"
                                + "  `pressure` DOUBLE,"
                                + "  WATERMARK FOR `timestamp` AS `timestamp`"
                                + ") WITH ("
                                + "  'connector' = 'opengemini',"
                                + "  'host' = '%s',"
                                + "  'port' = '%d',"
                                + "  'database' = '%s',"
                                + "  'measurement' = 'sensor_data',"
                                + "  'tag-fields' = 'sensor_id,location',"
                                + "  'field-fields' = 'temperature,humidity,pressure',"
                                + "  'timestamp-field' = 'timestamp',"
                                + "  'batch-size' = '100',"
                                + "  'flush-interval' = '1s'"
                                + ")",
                        host, port, TEST_DATABASE);

        tableEnv.executeSql(createSinkTable);

        // Create source table with test data
        String createSourceTable =
                "CREATE TABLE sensor_source ("
                        + "  `timestamp` TIMESTAMP(3),"
                        + "  `sensor_id` STRING,"
                        + "  `location` STRING,"
                        + "  `temperature` DOUBLE,"
                        + "  `humidity` DOUBLE,"
                        + "  `pressure` DOUBLE"
                        + ") WITH ("
                        + "  'connector' = 'datagen',"
                        + "  'rows-per-second' = '10',"
                        + "  'number-of-rows' = '100',"
                        + "  'fields.sensor_id.kind' = 'sequence',"
                        + "  'fields.sensor_id.start' = '1',"
                        + "  'fields.sensor_id.end' = '100',"
                        + "  'fields.location.length' = '10',"
                        + "  'fields.temperature.min' = '20.0',"
                        + "  'fields.temperature.max' = '30.0',"
                        + "  'fields.humidity.min' = '40.0',"
                        + "  'fields.humidity.max' = '80.0',"
                        + "  'fields.pressure.min' = '1000.0',"
                        + "  'fields.pressure.max' = '1020.0'"
                        + ")";

        tableEnv.executeSql(createSourceTable);

        // Insert data
        TableResult result =
                tableEnv.executeSql("INSERT INTO sensor_sink SELECT * FROM sensor_source");

        // Wait for data to be written
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(
                        () -> {
                            long count = countRecords("sensor_data");
                            log.info("Current record count: {}", count);
                            assertThat(count).isGreaterThanOrEqualTo(50);
                        });

        // Verify data integrity
        verifyDataIntegrity("sensor_data");
    }

    @Test
    @Order(2)
    @DisplayName("Test different data types")
    void testDataTypes() throws Exception {
        String createTable =
                String.format(
                        "CREATE TABLE type_test_sink ("
                                + "  `ts` TIMESTAMP(3),"
                                + "  `id` STRING,"
                                + "  `int_val` INT,"
                                + "  `bigint_val` BIGINT,"
                                + "  `float_val` FLOAT,"
                                + "  `double_val` DOUBLE,"
                                + "  `bool_val` BOOLEAN,"
                                + "  `string_val` STRING,"
                                + "  `decimal_val` DECIMAL(10, 2),"
                                + "  WATERMARK FOR `ts` AS `ts`"
                                + ") WITH ("
                                + "  'connector' = 'opengemini',"
                                + "  'host' = '%s',"
                                + "  'port' = '%d',"
                                + "  'database' = '%s',"
                                + "  'measurement' = 'type_test',"
                                + "  'tag-fields' = 'id',"
                                + "  'timestamp-field' = 'ts'"
                                + ")",
                        host, port, TEST_DATABASE);

        tableEnv.executeSql(createTable);

        // Insert test data with different types
        tableEnv.executeSql(
                "INSERT INTO type_test_sink VALUES "
                        + "(TIMESTAMP '2024-01-01 00:00:00', 'id1', 42, 100000, 3.14, 2.718, TRUE, 'test', 123.45),"
                        + "(TIMESTAMP '2024-01-01 00:00:01', 'id2', -10, -999999, -1.23, -456.78, FALSE, 'example', 999.99)");

        // Wait and verify
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(
                        () -> {
                            long count = countRecords("type_test");
                            assertThat(count).isEqualTo(2);
                        });

        // Verify all field types are correctly stored
        verifyFieldTypes("type_test");
    }

    @Test
    @Order(3)
    @DisplayName("Test SQL queries with aggregations")
    void testSQLAggregations() throws Exception {
        // Create table
        String createTable =
                String.format(
                        "CREATE TABLE metrics_sink ("
                                + "  `window_start` TIMESTAMP(3),"
                                + "  `window_end` TIMESTAMP(3),"
                                + "  `location` STRING,"
                                + "  `avg_temp` DOUBLE,"
                                + "  `max_temp` DOUBLE,"
                                + "  `min_temp` DOUBLE,"
                                + "  `record_count` BIGINT"
                                + ") WITH ("
                                + "  'connector' = 'opengemini',"
                                + "  'host' = '%s',"
                                + "  'port' = '%d',"
                                + "  'database' = '%s',"
                                + "  'measurement' = 'aggregated_metrics',"
                                + "  'tag-fields' = 'location',"
                                + "  'timestamp-field' = 'window_end'"
                                + ")",
                        host, port, TEST_DATABASE);

        tableEnv.executeSql(createTable);

        // Create source with tumbling window aggregation
        String createSource =
                "CREATE TABLE raw_metrics ("
                        + "  `ts` TIMESTAMP(3),"
                        + "  `location` STRING,"
                        + "  `temperature` DOUBLE,"
                        + "  WATERMARK FOR `ts` AS `ts` - INTERVAL '5' SECOND"
                        + ") WITH ("
                        + "  'connector' = 'datagen',"
                        + "  'rows-per-second' = '10',"
                        + "  'number-of-rows' = '100',"
                        + "  'fields.location.length' = '5',"
                        + "  'fields.temperature.min' = '15.0',"
                        + "  'fields.temperature.max' = '35.0'"
                        + ")";

        tableEnv.executeSql(createSource);

        // Perform windowed aggregation
        String aggregationQuery =
                "INSERT INTO metrics_sink "
                        + "SELECT "
                        + "  TUMBLE_START(ts, INTERVAL '10' SECOND) as window_start,"
                        + "  TUMBLE_END(ts, INTERVAL '10' SECOND) as window_end,"
                        + "  location,"
                        + "  AVG(temperature) as avg_temp,"
                        + "  MAX(temperature) as max_temp,"
                        + "  MIN(temperature) as min_temp,"
                        + "  COUNT(*) as record_count "
                        + "FROM raw_metrics "
                        + "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), location";

        tableEnv.executeSql(aggregationQuery);

        // Verify aggregated results
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(
                        () -> {
                            long count = countRecords("aggregated_metrics");
                            log.info("Aggregated records: {}", count);
                            assertThat(count).isGreaterThan(0);
                        });
    }

    @Test
    @Order(4)
    @DisplayName("Test field mapping configuration")
    void testFieldMapping() throws Exception {
        // Test custom field mapping
        String createTable =
                String.format(
                        "CREATE TABLE custom_mapping ("
                                + "  `event_time` TIMESTAMP(3),"
                                + "  `device_id` STRING,"
                                + "  `region` STRING,"
                                + "  `metric1` DOUBLE,"
                                + "  `metric2` DOUBLE,"
                                + "  `status` STRING"
                                + ") WITH ("
                                + "  'connector' = 'opengemini',"
                                + "  'host' = '%s',"
                                + "  'port' = '%d',"
                                + "  'database' = '%s',"
                                + "  'measurement' = 'custom_metrics',"
                                + "  'tag-fields' = 'device_id,region,status',"
                                + // Multiple tags
                                "  'field-fields' = 'metric1,metric2',"
                                + // Specific fields
                                "  'timestamp-field' = 'event_time',"
                                + "  'write-precision' = 'ms',"
                                + "  'ignore-null-values' = 'true'"
                                + ")",
                        host, port, TEST_DATABASE);

        tableEnv.executeSql(createTable);

        // Insert data with nulls
        tableEnv.executeSql(
                "INSERT INTO custom_mapping VALUES "
                        + "(TIMESTAMP '2024-01-01 00:00:00', 'dev1', 'us-east', 100.5, 200.3, 'active'),"
                        + "(TIMESTAMP '2024-01-01 00:00:01', 'dev2', 'us-west', CAST(NULL AS DOUBLE), 150.7, 'inactive'),"
                        + "(TIMESTAMP '2024-01-01 00:00:02', 'dev3', 'us-south', 75.2, CAST(NULL AS DOUBLE), 'active')");

        // query actual data after waiting failed
        try {
            await().atMost(Duration.ofSeconds(10))
                    .untilAsserted(
                            () -> {
                                long count = countRecords("custom_metrics");
                                assertThat(count).isEqualTo(3);
                            });
        } catch (Exception e) {
            Query query = new Query("SELECT * FROM custom_metrics");
            query.setDatabase(TEST_DATABASE);
            QueryResult result = client.query(query).get(5, TimeUnit.SECONDS);

            System.out.println("=== Actual data in OpenGemini ===");
            if (result.getResults().get(0).getSeries() != null) {
                for (Series series : result.getResults().get(0).getSeries()) {
                    System.out.println("Tags: " + series.getTags());
                    System.out.println("Columns: " + series.getColumns());
                    System.out.println("Values: " + series.getValues());
                }
            }
            throw e;
        }

        // Verify field mapping
        verifyFieldMapping("custom_metrics");
    }

    // Helper methods

    private long countRecords(String measurement) throws Exception {
        String countQuery = String.format("SELECT * FROM %s", measurement);
        Query query = new Query(countQuery);
        query.setDatabase(TEST_DATABASE);
        QueryResult result = client.query(query).get(5, TimeUnit.SECONDS);

        if (result.getResults().isEmpty()
                || result.getResults().get(0).getSeries() == null
                || result.getResults().get(0).getSeries().isEmpty()) {
            return 0;
        }

        long count = 0;
        for (Series series : result.getResults().get(0).getSeries()) {
            List<List<Object>> values = series.getValues();
            if (values != null) {
                count += values.size();
            }
        }

        log.info("Count of records: {}", count);

        return count;
    }

    private void verifyDataIntegrity(String measurement) throws Exception {
        String query = String.format("SELECT * FROM %s LIMIT 5", measurement);
        Query verificationQuery = new Query(query);
        verificationQuery.setDatabase(TEST_DATABASE);

        QueryResult result = client.query(verificationQuery).get(5, TimeUnit.SECONDS);
        assertThat(result.getResults()).isNotEmpty();

        if (result.getResults().get(0).getError() != null) {
            throw new AssertionError("Query error: " + result.getResults().get(0).getError());
        }

        List<Series> series = result.getResults().get(0).getSeries();
        assertThat(series).isNotEmpty();

        // Log sample data for debugging
        if (!series.get(0).getValues().isEmpty()) {
            log.info("Sample data from {}: {}", measurement, series.get(0).getValues().get(0));
        }
    }

    private void verifyFieldTypes(String measurement) throws Exception {
        String query = String.format("SELECT * FROM %s LIMIT 1", measurement);
        Query verificationQuery = new Query(query);
        verificationQuery.setDatabase(TEST_DATABASE);

        QueryResult result = client.query(verificationQuery).get(5, TimeUnit.SECONDS);
        assertThat(result.getResults()).isNotEmpty();

        List<Series> series = result.getResults().get(0).getSeries();
        assertThat(series).isNotEmpty();

        List<String> columns = series.get(0).getColumns();
        assertThat(columns)
                .contains(
                        "int_val",
                        "bigint_val",
                        "float_val",
                        "double_val",
                        "bool_val",
                        "string_val",
                        "decimal_val");

        // Verify values
        List<List<Object>> values = series.get(0).getValues();
        assertThat(values).isNotEmpty();
        log.info("Field types test - columns: {}, values: {}", columns, values.get(0));
    }

    private void verifyFieldMapping(String measurement) throws Exception {
        // Verify tags
        Query tagQuery = new Query(String.format("SHOW TAG KEYS FROM %s", measurement));
        tagQuery.setDatabase(TEST_DATABASE);
        QueryResult tagResult = client.query(tagQuery).get(5, TimeUnit.SECONDS);
        log.info("Tag keys in {}: {}", measurement, tagResult);

        List<Series> tagSeries = tagResult.getResults().get(0).getSeries();
        assertThat(tagSeries).isNotEmpty();

        List<List<Object>> tagValues = tagSeries.get(0).getValues();
        List<String> tags =
                tagValues.stream()
                        .map(v -> (String) v.get(0))
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(tags).containsExactlyInAnyOrder("device_id", "region", "status");

        // Verify fields
        Query fieldQuery = new Query(String.format("SHOW FIELD KEYS FROM %s", measurement));
        fieldQuery.setDatabase(TEST_DATABASE);
        QueryResult fieldResult = client.query(fieldQuery).get(5, TimeUnit.SECONDS);

        List<Series> fieldSeries = fieldResult.getResults().get(0).getSeries();
        assertThat(fieldSeries).isNotEmpty();

        List<List<Object>> fieldValues = fieldSeries.get(0).getValues();
        List<String> fields =
                fieldValues.stream()
                        .map(v -> (String) v.get(0))
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(fields).containsExactlyInAnyOrder("metric1", "metric2");
    }
}
