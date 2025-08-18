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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengemini.flink.sink.OpenGeminiSinkConfiguration;
import org.opengemini.flink.table.OpenGeminiDynamicTableSink.RowDataToPointConverter;
import org.opengemini.flink.table.OpenGeminiDynamicTableSinkFactory.FieldMappingConfig;

import io.opengemini.client.api.Point;
import io.opengemini.client.api.Precision;

/** Tests for OpenGeminiDynamicTableSink and RowDataToPointConverter. */
class OpenGeminiDynamicTableSinkTest {

    private ResolvedSchema resolvedSchema;

    @BeforeEach
    void setUp() {
        resolvedSchema = mock(ResolvedSchema.class);
    }

    @Test
    void testRowDataToPointConverter_basicConversion_tagsAndFieldsAndTimestamp() {
        // Columns: id (STRING), ts (BIGINT), tag (STRING), val (DOUBLE)
        List<String> columnNames = Arrays.asList("id", "ts", "tag", "val");
        List<DataType> columnTypes =
                Arrays.asList(
                        DataTypes.STRING(),
                        DataTypes.BIGINT(),
                        DataTypes.STRING(),
                        DataTypes.DOUBLE());

        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fm =
                FieldMappingConfig.builder()
                        .timestampField("ts")
                        .addTagField("tag")
                        .ignoreNullValues(true)
                        .writePrecision("ms")
                        .build();

        RowDataToPointConverter converter =
                new RowDataToPointConverter(resolvedSchema, fm, "measurement1");

        // Build a GenericRowData: id="id1", ts=1000L, tag="sensor", val=2.5
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("id1"), 1000L, StringData.fromString("sensor"), 2.5d);
        row.setRowKind(RowKind.INSERT);

        Point point = converter.convert(row, "measurement1");
        assertNotNull(point);
        assertEquals("measurement1", point.getMeasurement());

        // timestamp: BIGINT 1000 with ms precision -> convertToNanos = 1000 * 1_000_000
        long expectedTime = 1000L * 1_000_000L;
        assertEquals(expectedTime, point.getTime());

        // precision should be MILLISECOND
        assertEquals(Precision.PRECISIONMILLISECOND, point.getPrecision());

        Map<String, String> tags = point.getTags();
        assertNotNull(tags);
        assertEquals("sensor", tags.get("tag"));

        Map<String, Object> fields = point.getFields();
        assertNotNull(fields);
        // fieldColumns by default: id and val (non-tag, non-timestamp)
        assertTrue(fields.containsKey("id"));
        assertTrue(fields.containsKey("val"));
        assertEquals("id1", fields.get("id"));
        assertEquals(2.5d, (Double) fields.get("val"));
    }

    @Test
    void testRowDataToPointConverter_deleteRow_returnsNull() {
        // minimal schema
        List<String> columnNames = Collections.singletonList("c1");
        List<DataType> columnTypes = Collections.singletonList(DataTypes.STRING());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fm = FieldMappingConfig.builder().build();
        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        GenericRowData row = GenericRowData.of(StringData.fromString("x"));
        row.setRowKind(RowKind.DELETE);

        Point p = converter.convert(row, "m");
        assertNull(p);
    }

    @Test
    void testRowDataToPointConverter_emptyFields_insertsEmptyFlag() {
        // Columns: only tag and timestamp, but fieldColumns will be empty -> _empty added
        List<String> columnNames = Arrays.asList("tagOnly", "ts");
        List<DataType> columnTypes = Arrays.asList(DataTypes.STRING(), DataTypes.BIGINT());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fm =
                FieldMappingConfig.builder()
                        .timestampField("ts")
                        .addTagField("tagOnly")
                        .ignoreNullValues(true)
                        .writePrecision("ms")
                        .build();

        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        // Provide a row where tagOnly is "t" and timestamp is 10L; no other field
        GenericRowData row = GenericRowData.of(StringData.fromString("t"), 10L);
        row.setRowKind(RowKind.INSERT);

        Point p = converter.convert(row, "m");
        assertNotNull(p);
        Map<String, Object> fields = p.getFields();
        assertNotNull(fields);
        assertTrue(fields.containsKey("_empty"));
        assertEquals(Boolean.TRUE, fields.get("_empty"));
    }

    @Test
    void testRowDataToPointConverter_precisionMapping_us_ns() {
        // Columns: id (STRING)
        List<String> columnNames = Collections.singletonList("id");
        List<DataType> columnTypes = Collections.singletonList(DataTypes.STRING());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        // microsecond
        FieldMappingConfig fmUs = FieldMappingConfig.builder().writePrecision("us").build();
        RowDataToPointConverter convUs = new RowDataToPointConverter(resolvedSchema, fmUs, "m");
        GenericRowData rowUs = GenericRowData.of(StringData.fromString("a"));
        rowUs.setRowKind(RowKind.INSERT);
        Point pUs = convUs.convert(rowUs, "m");
        assertNotNull(pUs);
        assertEquals(Precision.PRECISIONMICROSECOND, pUs.getPrecision());

        // nanosecond
        FieldMappingConfig fmNs = FieldMappingConfig.builder().writePrecision("ns").build();
        RowDataToPointConverter convNs = new RowDataToPointConverter(resolvedSchema, fmNs, "m");
        GenericRowData rowNs = GenericRowData.of(StringData.fromString("b"));
        rowNs.setRowKind(RowKind.INSERT);
        Point pNs = convNs.convert(rowNs, "m");
        assertNotNull(pNs);
        assertEquals(Precision.PRECISIONNANOSECOND, pNs.getPrecision());
    }

    @Test
    void testOpenGeminiDynamicTableSink_getChangelogMode() {
        OpenGeminiSinkConfiguration<RowData> config = mock(OpenGeminiSinkConfiguration.class);
        ResolvedSchema schema = mock(ResolvedSchema.class);
        FieldMappingConfig fieldMapping = FieldMappingConfig.builder().build();

        OpenGeminiDynamicTableSink sink =
                new OpenGeminiDynamicTableSink(config, schema, fieldMapping, null);

        ChangelogMode mode = sink.getChangelogMode(ChangelogMode.all());

        assertTrue(mode.contains(RowKind.INSERT));
        assertTrue(mode.contains(RowKind.UPDATE_AFTER));
        assertFalse(mode.contains(RowKind.DELETE));
        assertFalse(mode.contains(RowKind.UPDATE_BEFORE));
    }

    @Test
    void testOpenGeminiDynamicTableSink_copy() {
        OpenGeminiSinkConfiguration<RowData> config = mock(OpenGeminiSinkConfiguration.class);
        ResolvedSchema schema = mock(ResolvedSchema.class);
        FieldMappingConfig fieldMapping = FieldMappingConfig.builder().build();
        Integer parallelism = 4;

        OpenGeminiDynamicTableSink sink =
                new OpenGeminiDynamicTableSink(config, schema, fieldMapping, parallelism);

        DynamicTableSink copiedSink = sink.copy();

        assertNotNull(copiedSink);
        assertTrue(copiedSink instanceof OpenGeminiDynamicTableSink);
        assertNotSame(sink, copiedSink);
    }

    @Test
    void testOpenGeminiDynamicTableSink_asSummaryString() {
        OpenGeminiSinkConfiguration<RowData> config = mock(OpenGeminiSinkConfiguration.class);
        when(config.getHost()).thenReturn("localhost");
        when(config.getPort()).thenReturn(8086);
        when(config.getDatabase()).thenReturn("testdb");
        when(config.getMeasurement()).thenReturn("metrics");

        ResolvedSchema schema = mock(ResolvedSchema.class);
        FieldMappingConfig fieldMapping = FieldMappingConfig.builder().build();

        OpenGeminiDynamicTableSink sink =
                new OpenGeminiDynamicTableSink(config, schema, fieldMapping, null);

        String summary = sink.asSummaryString();

        assertEquals(
                "OpenGemini Table Sink(host=localhost:8086, database=testdb, measurement=metrics)",
                summary);
    }

    @Test
    void testOpenGeminiDynamicTableSink_getSinkRuntimeProvider() {
        // Create mock configuration
        OpenGeminiSinkConfiguration<RowData> config = mock(OpenGeminiSinkConfiguration.class);
        when(config.getMeasurement()).thenReturn("test_measurement");

        // Create mock builder that returns a new config
        OpenGeminiSinkConfiguration.Builder<RowData> builder =
                mock(OpenGeminiSinkConfiguration.Builder.class);
        when(config.toBuilder()).thenReturn(builder);
        when(builder.converter(any())).thenReturn(builder);
        when(builder.build()).thenReturn(config);

        // Create schema
        List<String> columnNames = Arrays.asList("id", "value");
        List<DataType> columnTypes = Arrays.asList(DataTypes.STRING(), DataTypes.DOUBLE());
        ResolvedSchema schema = mock(ResolvedSchema.class);
        when(schema.getColumnNames()).thenReturn(columnNames);
        when(schema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fieldMapping = FieldMappingConfig.builder().build();

        OpenGeminiDynamicTableSink sink =
                new OpenGeminiDynamicTableSink(config, schema, fieldMapping, 2);

        DynamicTableSink.Context context = mock(DynamicTableSink.Context.class);
        DynamicTableSink.SinkRuntimeProvider provider = sink.getSinkRuntimeProvider(context);

        assertNotNull(provider);
        assertTrue(provider instanceof SinkFunctionProvider);
    }

    @Test
    void testRowDataToPointConverter_updateBefore_returnsNull() {
        List<String> columnNames = Collections.singletonList("c1");
        List<DataType> columnTypes = Collections.singletonList(DataTypes.STRING());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fm = FieldMappingConfig.builder().build();
        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        GenericRowData row = GenericRowData.of(StringData.fromString("x"));
        row.setRowKind(RowKind.UPDATE_BEFORE);

        Point p = converter.convert(row, "m");
        assertNull(p);
    }

    @Test
    void testRowDataToPointConverter_noTimestampField_usesCurrentTime() {
        List<String> columnNames = Collections.singletonList("value");
        List<DataType> columnTypes = Collections.singletonList(DataTypes.DOUBLE());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        // No timestamp field specified
        FieldMappingConfig fm = FieldMappingConfig.builder().build();
        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        GenericRowData row = GenericRowData.of(3.14);
        row.setRowKind(RowKind.INSERT);

        long beforeConvert = System.currentTimeMillis();
        Point p = converter.convert(row, "m");
        long afterConvert = System.currentTimeMillis();

        assertNotNull(p);
        // extractTimestamp returns milliseconds when no timestamp field
        long pointTime = p.getTime(); // Already in millis
        assertTrue(pointTime >= beforeConvert && pointTime <= afterConvert + 100);
    }

    @Test
    void testRowDataToPointConverter_specifiedFieldFields() {
        List<String> columnNames = Arrays.asList("tag1", "field1", "field2", "field3");
        List<DataType> columnTypes =
                Arrays.asList(
                        DataTypes.STRING(), DataTypes.INT(), DataTypes.DOUBLE(), DataTypes.FLOAT());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        // Explicitly specify which fields to use
        FieldMappingConfig fm =
                FieldMappingConfig.builder()
                        .addTagField("tag1")
                        .addFieldField("field1")
                        .addFieldField("field3") // Skip field2
                        .build();

        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        GenericRowData row = GenericRowData.of(StringData.fromString("tag_value"), 10, 20.0, 30.0f);
        row.setRowKind(RowKind.INSERT);

        Point p = converter.convert(row, "m");
        assertNotNull(p);

        Map<String, Object> fields = p.getFields();
        assertTrue(fields.containsKey("field1"));
        assertFalse(fields.containsKey("field2")); // Not included
        assertTrue(fields.containsKey("field3"));
        assertEquals(10, fields.get("field1"));
        assertEquals(30.0f, fields.get("field3"));
    }

    @Test
    void testRowDataToPointConverter_nullHandling() {
        List<String> columnNames = Arrays.asList("tag", "field1", "field2");
        List<DataType> columnTypes =
                Arrays.asList(DataTypes.STRING(), DataTypes.INT(), DataTypes.DOUBLE());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        // Test with ignoreNullValues = false
        FieldMappingConfig fm =
                FieldMappingConfig.builder().addTagField("tag").ignoreNullValues(false).build();

        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        // Create row with null field2
        GenericRowData row = new GenericRowData(3);
        row.setField(0, StringData.fromString("tag_value"));
        row.setField(1, 10);
        row.setField(2, null); // null field
        row.setRowKind(RowKind.INSERT);

        Point p = converter.convert(row, "m");
        assertNotNull(p);

        Map<String, Object> fields = p.getFields();
        assertTrue(fields.containsKey("field1"));
        assertFalse(
                fields.containsKey("field2")); // Even with ignoreNullValues=false, null is skipped
    }

    @Test
    void testRowDataToPointConverter_allPrecisions() {
        List<String> columnNames = Collections.singletonList("value");
        List<DataType> columnTypes = Collections.singletonList(DataTypes.STRING());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        // Test all precision types
        String[] precisions = {"s", "m", "h"};
        Precision[] expectedPrecisions = {
            Precision.PRECISIONSECOND, Precision.PRECISIONMINUTE, Precision.PRECISIONHOUR
        };

        for (int i = 0; i < precisions.length; i++) {
            FieldMappingConfig fm =
                    FieldMappingConfig.builder().writePrecision(precisions[i]).build();
            RowDataToPointConverter converter =
                    new RowDataToPointConverter(resolvedSchema, fm, "m");

            GenericRowData row = GenericRowData.of(StringData.fromString("test"));
            row.setRowKind(RowKind.INSERT);

            Point p = converter.convert(row, "m");
            assertNotNull(p);
            assertEquals(expectedPrecisions[i], p.getPrecision());
        }
    }

    @Test
    void testRowDataToPointConverter_timestampTypes() {
        List<String> columnNames = Arrays.asList("ts", "value");
        List<DataType> columnTypes = Arrays.asList(DataTypes.TIMESTAMP(3), DataTypes.STRING());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fm =
                FieldMappingConfig.builder().timestampField("ts").writePrecision("ms").build();

        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        // Create timestamp data
        TimestampData timestamp =
                TimestampData.fromEpochMillis(1609459200000L); // 2021-01-01 00:00:00
        GenericRowData row = GenericRowData.of(timestamp, StringData.fromString("test"));
        row.setRowKind(RowKind.INSERT);

        Point p = converter.convert(row, "m");
        assertNotNull(p);
        // extractTimestamp returns milliseconds, not nanos
        assertEquals(1609459200000L, p.getTime()); // Time is in milliseconds
    }

    @Test
    void testRowDataToPointConverter_variousDataTypes() {
        List<String> columnNames =
                Arrays.asList(
                        "bool_col",
                        "byte_col",
                        "short_col",
                        "int_col",
                        "long_col",
                        "float_col",
                        "double_col",
                        "decimal_col",
                        "char_col");
        List<DataType> columnTypes =
                Arrays.asList(
                        DataTypes.BOOLEAN(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(10, 2),
                        DataTypes.CHAR(10));
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fm = FieldMappingConfig.builder().build();
        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        GenericRowData row =
                GenericRowData.of(
                        true, // boolean
                        (byte) 1, // tinyint
                        (short) 10, // smallint
                        100, // int
                        1000L, // bigint
                        3.14f, // float
                        2.718, // double
                        DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2), // decimal
                        StringData.fromString("test") // char
                        );
        row.setRowKind(RowKind.INSERT);

        Point p = converter.convert(row, "m");
        assertNotNull(p);

        Map<String, Object> fields = p.getFields();
        assertEquals(true, fields.get("bool_col"));
        assertEquals(1, fields.get("byte_col")); // Converted to int
        assertEquals(10, fields.get("short_col")); // Converted to int
        assertEquals(100, fields.get("int_col"));
        assertEquals(1000L, fields.get("long_col"));
        assertEquals(3.14f, fields.get("float_col"));
        assertEquals(2.718, fields.get("double_col"));
        assertNotNull(fields.get("decimal_col"));
        assertEquals("test", fields.get("char_col"));
    }

    @Test
    void testRowDataToPointConverter_nullTagSkipped() {
        List<String> columnNames = Arrays.asList("tag1", "tag2", "value");
        List<DataType> columnTypes =
                Arrays.asList(DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fm =
                FieldMappingConfig.builder().addTagField("tag1").addTagField("tag2").build();

        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        // Create row with null tag1
        GenericRowData row = new GenericRowData(3);
        row.setField(0, null); // null tag
        row.setField(1, StringData.fromString("tag2_value"));
        row.setField(2, 42);
        row.setRowKind(RowKind.INSERT);

        Point p = converter.convert(row, "m");
        assertNotNull(p);

        Map<String, String> tags = p.getTags();
        assertNotNull(tags);
        assertFalse(tags.containsKey("tag1")); // Null tag is skipped
        assertTrue(tags.containsKey("tag2"));
        assertEquals("tag2_value", tags.get("tag2"));
    }

    @Test
    void testRowDataToPointConverter_tagAsString() {
        // Test that various data types can be converted to string for tags
        List<String> columnNames = Arrays.asList("int_tag", "bool_tag", "double_tag", "value");
        List<DataType> columnTypes =
                Arrays.asList(
                        DataTypes.INT(),
                        DataTypes.BOOLEAN(),
                        DataTypes.DOUBLE(),
                        DataTypes.STRING());
        when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
        when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

        FieldMappingConfig fm =
                FieldMappingConfig.builder()
                        .addTagField("int_tag")
                        .addTagField("bool_tag")
                        .addTagField("double_tag")
                        .build();

        RowDataToPointConverter converter = new RowDataToPointConverter(resolvedSchema, fm, "m");

        GenericRowData row = GenericRowData.of(123, true, 3.14, StringData.fromString("test"));
        row.setRowKind(RowKind.INSERT);

        Point p = converter.convert(row, "m");
        assertNotNull(p);

        Map<String, String> tags = p.getTags();
        assertEquals("123", tags.get("int_tag"));
        assertEquals("true", tags.get("bool_tag"));
        assertEquals("3.14", tags.get("double_tag"));
    }
}
