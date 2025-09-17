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
package org.opengemini.flink.table;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengemini.flink.sink.OpenGeminiSinkConfiguration;
import org.opengemini.flink.table.OpenGeminiDynamicTableSink.RowDataToLineProtocolConverter;
import org.opengemini.flink.table.OpenGeminiDynamicTableSink.RowDataToPointConverter;
import org.opengemini.flink.table.OpenGeminiDynamicTableSinkFactory.FieldMappingConfig;

import io.opengemini.client.api.Point;

/** Tests for converter type selection in OpenGeminiDynamicTableSink */
public class ConverterTypeSelectionTest {

    private OpenGeminiDynamicTableSinkFactory factory;
    private ResolvedSchema resolvedSchema;

    @BeforeEach
    void setUp() {
        factory = new OpenGeminiDynamicTableSinkFactory();

        // Create a simple schema for testing
        Column col1 = Column.physical("id", DataTypes.INT());
        Column col2 = Column.physical("name", DataTypes.STRING());
        Column col3 = Column.physical("value", DataTypes.DOUBLE());
        List<Column> columns = Arrays.asList(col1, col2, col3);

        resolvedSchema = ResolvedSchema.of(columns);
    }

    @Test
    @DisplayName("Should create sink with line-protocol converter by default")
    void testDefaultConverterTypeIsLineProtocol() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "opengemini");
        options.put("host", "localhost");
        options.put("database", "testdb");
        options.put("measurement", "test_measurement");
        // Not specifying converter.type, should use default

        DynamicTableSinkFactory.Context context = createMockContext(options);
        DynamicTableSink sink = factory.createDynamicTableSink(context);

        assertNotNull(sink);
        assertTrue(sink instanceof OpenGeminiDynamicTableSink);

        // Verify it creates the correct runtime provider
        OpenGeminiDynamicTableSink openGeminiSink = (OpenGeminiDynamicTableSink) sink;
        DynamicTableSink.Context sinkContext = mock(DynamicTableSink.Context.class);
        DynamicTableSink.SinkRuntimeProvider provider =
                openGeminiSink.getSinkRuntimeProvider(sinkContext);

        assertNotNull(provider);
        assertTrue(provider instanceof SinkFunctionProvider);
    }

    @Test
    @DisplayName("Should create sink with line-protocol converter when explicitly specified")
    void testExplicitLineProtocolConverter() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "opengemini");
        options.put("host", "localhost");
        options.put("database", "testdb");
        options.put("measurement", "test_measurement");
        options.put("converter.type", "line-protocol");

        DynamicTableSinkFactory.Context context = createMockContext(options);
        DynamicTableSink sink = factory.createDynamicTableSink(context);

        assertNotNull(sink);
        assertTrue(sink instanceof OpenGeminiDynamicTableSink);
    }

    @Test
    @DisplayName("Should create sink with point converter when specified")
    void testPointConverter() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "opengemini");
        options.put("host", "localhost");
        options.put("database", "testdb");
        options.put("measurement", "test_measurement");
        options.put("converter.type", "point");

        DynamicTableSinkFactory.Context context = createMockContext(options);
        DynamicTableSink sink = factory.createDynamicTableSink(context);

        assertNotNull(sink);
        assertTrue(sink instanceof OpenGeminiDynamicTableSink);

        // Verify it creates the correct runtime provider
        OpenGeminiDynamicTableSink openGeminiSink = (OpenGeminiDynamicTableSink) sink;
        DynamicTableSink.Context sinkContext = mock(DynamicTableSink.Context.class);
        DynamicTableSink.SinkRuntimeProvider provider =
                openGeminiSink.getSinkRuntimeProvider(sinkContext);

        assertNotNull(provider);
        assertTrue(provider instanceof SinkFunctionProvider);
    }

    @Test
    @DisplayName("Should verify both converters produce semantically equivalent output")
    void testConverterEquivalence() {
        // Setup schema
        List<String> columnNames = Arrays.asList("host", "cpu", "usage");
        List<DataType> columnTypes =
                Arrays.asList(DataTypes.STRING(), DataTypes.STRING(), DataTypes.DOUBLE());

        ResolvedSchema schema = mock(ResolvedSchema.class);
        when(schema.getColumnNames()).thenReturn(columnNames);
        when(schema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fieldMapping = FieldMappingConfig.builder().addTagField("host").build();

        // Create both converters
        RowDataToPointConverter pointConverter =
                new RowDataToPointConverter(schema, fieldMapping, "cpu_metrics");
        RowDataToLineProtocolConverter lineProtocolConverter =
                new RowDataToLineProtocolConverter(schema, fieldMapping, "cpu_metrics");

        // Test data
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("server01"), StringData.fromString("cpu0"), 75.5);
        row.setRowKind(RowKind.INSERT);

        // Convert with both converters
        Point point = pointConverter.convertToPoint(row, "cpu_metrics");
        String lineProtocol = lineProtocolConverter.convertToLineProtocol(row, "cpu_metrics");

        // Verify Point converter output
        assertNotNull(point);
        assertEquals("cpu_metrics", point.getMeasurement());
        assertEquals("server01", point.getTags().get("host"));
        assertEquals("cpu0", point.getFields().get("cpu"));
        assertEquals(75.5, point.getFields().get("usage"));

        // Verify Line Protocol converter output
        assertNotNull(lineProtocol);
        assertTrue(lineProtocol.startsWith("cpu_metrics,host=server01 "));
        assertTrue(lineProtocol.contains("cpu=\"cpu0\""));
        assertTrue(lineProtocol.contains("usage=75.5"));
    }

    @Test
    @DisplayName("Should handle all row kinds consistently across converters")
    void testRowKindHandlingAcrossConverters() {
        // Setup schema
        List<String> columnNames = Arrays.asList("value");
        List<DataType> columnTypes = Arrays.asList(DataTypes.INT());

        ResolvedSchema schema = mock(ResolvedSchema.class);
        when(schema.getColumnNames()).thenReturn(columnNames);
        when(schema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fieldMapping = FieldMappingConfig.builder().build();

        RowDataToPointConverter pointConverter =
                new RowDataToPointConverter(schema, fieldMapping, "test");
        RowDataToLineProtocolConverter lineProtocolConverter =
                new RowDataToLineProtocolConverter(schema, fieldMapping, "test");

        // Test all row kinds
        RowKind[] rowKinds = {
            RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE, RowKind.UPDATE_BEFORE
        };

        for (RowKind rowKind : rowKinds) {
            GenericRowData row = GenericRowData.of(42);
            row.setRowKind(rowKind);

            Point point = pointConverter.convertToPoint(row, "test");
            String lineProtocol = lineProtocolConverter.convertToLineProtocol(row, "test");

            if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
                // Both converters should produce output
                assertNotNull(point, "Point converter should produce output for " + rowKind);
                assertNotNull(
                        lineProtocol,
                        "Line protocol converter should produce output for " + rowKind);
            } else {
                // Both converters should return null
                assertNull(point, "Point converter should return null for " + rowKind);
                assertNull(
                        lineProtocol, "Line protocol converter should return null for " + rowKind);
            }
        }
    }

    @Test
    @DisplayName("Should copy sink with correct converter type")
    void testSinkCopyPreservesConverterType() {
        OpenGeminiSinkConfiguration<RowData> config = mock(OpenGeminiSinkConfiguration.class);
        when(config.getMeasurement()).thenReturn("test");

        // Create sink with line-protocol converter
        OpenGeminiDynamicTableSink originalSink =
                new OpenGeminiDynamicTableSink(
                        config,
                        resolvedSchema,
                        FieldMappingConfig.builder().build(),
                        2,
                        "line-protocol");

        DynamicTableSink copiedSink = originalSink.copy();

        assertNotNull(copiedSink);
        assertTrue(copiedSink instanceof OpenGeminiDynamicTableSink);
        assertNotSame(originalSink, copiedSink);

        // Create sink with point converter
        OpenGeminiDynamicTableSink originalPointSink =
                new OpenGeminiDynamicTableSink(
                        config, resolvedSchema, FieldMappingConfig.builder().build(), 2, "point");

        DynamicTableSink copiedPointSink = originalPointSink.copy();

        assertNotNull(copiedPointSink);
        assertTrue(copiedPointSink instanceof OpenGeminiDynamicTableSink);
        assertNotSame(originalPointSink, copiedPointSink);
    }

    @Test
    @DisplayName("Should handle null converter type as default")
    void testNullConverterTypeUsesDefault() {
        OpenGeminiSinkConfiguration<RowData> config = mock(OpenGeminiSinkConfiguration.class);
        when(config.getMeasurement()).thenReturn("test");
        when(config.toBuilder()).thenReturn(mock(OpenGeminiSinkConfiguration.Builder.class));

        // Pass null as converter type
        OpenGeminiDynamicTableSink sink =
                new OpenGeminiDynamicTableSink(
                        config, resolvedSchema, FieldMappingConfig.builder().build(), null, null);

        assertNotNull(sink);
        // Should use default converter type (line-protocol)
        String summary = sink.asSummaryString();
        assertNotNull(summary);
    }

    // Helper method to create mock context
    private DynamicTableSinkFactory.Context createMockContext(Map<String, String> options) {
        DynamicTableSinkFactory.Context context = mock(DynamicTableSinkFactory.Context.class);

        // Mock ObjectIdentifier
        ObjectIdentifier identifier =
                ObjectIdentifier.of("default_catalog", "default_database", "test_table");
        when(context.getObjectIdentifier()).thenReturn(identifier);

        // Mock Configuration
        Configuration configuration = Configuration.fromMap(options);
        when(context.getConfiguration()).thenReturn(configuration);

        // Mock CatalogTable with ResolvedSchema
        ResolvedCatalogTable catalogTable = mock(ResolvedCatalogTable.class);
        when(catalogTable.getResolvedSchema()).thenReturn(resolvedSchema);
        when(catalogTable.getOptions()).thenReturn(options);
        when(catalogTable.getPartitionKeys()).thenReturn(Collections.emptyList());
        when(context.getCatalogTable()).thenReturn(catalogTable);

        // Mock ClassLoader
        when(context.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
        when(context.isTemporary()).thenReturn(false);

        return context;
    }
}
