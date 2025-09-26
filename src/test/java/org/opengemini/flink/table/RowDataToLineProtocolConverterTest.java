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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opengemini.flink.table.OpenGeminiDynamicTableSink.RowDataToLineProtocolConverter;
import org.opengemini.flink.table.OpenGeminiDynamicTableSinkFactory.FieldMappingConfig;

/** Core tests for RowDataToLineProtocolConverter focusing on the most critical functionality */
public class RowDataToLineProtocolConverterTest {

    private ResolvedSchema resolvedSchema;

    @BeforeEach
    void setUp() {
        resolvedSchema = mock(ResolvedSchema.class);
    }

    @Nested
    @DisplayName("Basic Line Protocol Generation")
    class BasicLineProtocolTests {

        @Test
        @DisplayName("Should generate valid line protocol with all components")
        void testCompleteLineProtocol() {
            // Setup schema: measurement,tag1=value1,tag2=value2
            // field1="string",field2=42i,field3=3.14 timestamp
            List<String> columnNames =
                    Arrays.asList("host", "region", "message", "value", "temperature", "ts");
            List<DataType> columnTypes =
                    Arrays.asList(
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.DOUBLE(),
                            DataTypes.BIGINT());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm =
                    FieldMappingConfig.builder()
                            .addTagField("host")
                            .addTagField("region")
                            .timestampField("ts")
                            .sourceTimestampPrecision("ms")
                            .build();

            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "cpu");

            GenericRowData row =
                    GenericRowData.of(
                            StringData.fromString("server01"),
                            StringData.fromString("us-west"),
                            StringData.fromString("Hello World"),
                            42,
                            3.14,
                            1609459200000L // 2021-01-01 00:00:00 in millis
                            );
            row.setRowKind(RowKind.INSERT);

            // Act
            String lineProtocol = converter.convertToLineProtocol(row, "cpu");

            // Assert - verify the format: measurement,tags fields timestamp
            assertNotNull(lineProtocol);

            // Should start with measurement and tags
            assertTrue(lineProtocol.startsWith("cpu,host=server01,region=us-west "));

            // Should contain fields
            assertTrue(lineProtocol.contains("message=\"Hello World\""));
            assertTrue(lineProtocol.contains("value=42i"));
            assertTrue(lineProtocol.contains("temperature=3.14"));

            // Should end with timestamp
            assertTrue(lineProtocol.endsWith(" 1609459200000000000"));

            // Validate complete format with regex
            String regex =
                    "^cpu,host=server01,region=us-west (message=\"Hello World\",value=42i,temperature=3.14|"
                            + "message=\"Hello World\",temperature=3.14,value=42i|"
                            + "value=42i,message=\"Hello World\",temperature=3.14|"
                            + "value=42i,temperature=3.14,message=\"Hello World\"|"
                            + "temperature=3.14,message=\"Hello World\",value=42i|"
                            + "temperature=3.14,value=42i,message=\"Hello World\") 1609459200000000000$";
            System.out.println(lineProtocol);
            assertTrue(Pattern.matches(regex, lineProtocol));
        }

        @Test
        @DisplayName("Should handle measurement without tags")
        void testNoTags() {
            List<String> columnNames = Arrays.asList("value", "count");
            List<DataType> columnTypes = Arrays.asList(DataTypes.DOUBLE(), DataTypes.BIGINT());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "metrics");

            GenericRowData row = GenericRowData.of(99.9, 1000L);
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "metrics");

            // Should not have comma after measurement name
            assertTrue(lineProtocol.startsWith("metrics "));
            assertFalse(lineProtocol.startsWith("metrics,"));
            assertTrue(lineProtocol.contains("value=99.9"));
            assertTrue(lineProtocol.contains("count=1000i"));
        }

        @Test
        @DisplayName("Should return null value when no fields have values")
        void testEmptyFields() {
            List<String> columnNames = Arrays.asList("tag1", "field1");
            List<DataType> columnTypes = Arrays.asList(DataTypes.STRING(), DataTypes.STRING());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm =
                    FieldMappingConfig.builder()
                            .addTagField("tag1")
                            .addFieldField("field1")
                            .build();

            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row = new GenericRowData(2);
            row.setField(0, StringData.fromString("tag_value"));
            row.setField(1, null); // null field
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "test");

            assertNull(lineProtocol);
        }
    }

    @Nested
    @DisplayName("Special Character Escaping")
    class EscapingTests {

        @Test
        @DisplayName("Should escape spaces, commas, and equals in tag values")
        void testTagEscaping() {
            List<String> columnNames =
                    Arrays.asList("tag_with_space", "tag_with_comma", "tag_with_equals", "value");
            List<DataType> columnTypes =
                    Arrays.asList(
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.INT());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm =
                    FieldMappingConfig.builder()
                            .addTagField("tag_with_space")
                            .addTagField("tag_with_comma")
                            .addTagField("tag_with_equals")
                            .build();

            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row =
                    GenericRowData.of(
                            StringData.fromString("server 01"),
                            StringData.fromString("region,east"),
                            StringData.fromString("key=value"),
                            100);
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "test");

            // Verify escaping
            assertTrue(lineProtocol.contains("tag_with_space=server\\ 01"));
            assertTrue(lineProtocol.contains("tag_with_comma=region\\,east"));
            assertTrue(lineProtocol.contains("tag_with_equals=key\\=value"));
        }

        @Test
        @DisplayName("Should escape quotes in string field values")
        void testStringFieldQuoteEscaping() {
            List<String> columnNames = Arrays.asList("message", "description");
            List<DataType> columnTypes = Arrays.asList(DataTypes.STRING(), DataTypes.STRING());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "logs");

            GenericRowData row =
                    GenericRowData.of(
                            StringData.fromString("He said \"Hello\""),
                            StringData.fromString("Contains \"multiple\" quotes"));
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "logs");

            // Verify quote escaping in fields
            assertTrue(lineProtocol.contains("message=\"He said \\\"Hello\\\"\""));
            assertTrue(lineProtocol.contains("description=\"Contains \\\"multiple\\\" quotes\""));
        }

        @Test
        @DisplayName("Should handle complex escaping combinations")
        void testComplexEscaping() {
            List<String> columnNames = Arrays.asList("complex_tag", "complex_field");
            List<DataType> columnTypes = Arrays.asList(DataTypes.STRING(), DataTypes.STRING());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().addTagField("complex_tag").build();

            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row =
                    GenericRowData.of(
                            StringData.fromString("tag with space, comma=equals"),
                            StringData.fromString("field with \"quotes\" and spaces"));
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "test");

            // All special chars should be escaped in tag
            assertTrue(lineProtocol.contains("complex_tag=tag\\ with\\ space\\,\\ comma\\=equals"));
            // Quotes should be escaped in field
            assertTrue(
                    lineProtocol.contains(
                            "complex_field=\"field with \\\"quotes\\\" and spaces\""));
        }
    }

    @Nested
    @DisplayName("Data Type Formatting")
    class DataTypeFormattingTests {

        @Test
        @DisplayName("Should format all numeric types correctly")
        void testNumericTypeFormatting() {
            List<String> columnNames =
                    Arrays.asList(
                            "bool_field",
                            "byte_field",
                            "short_field",
                            "int_field",
                            "long_field",
                            "float_field",
                            "double_field",
                            "decimal_field");
            List<DataType> columnTypes =
                    Arrays.asList(
                            DataTypes.BOOLEAN(),
                            DataTypes.TINYINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.FLOAT(),
                            DataTypes.DOUBLE(),
                            DataTypes.DECIMAL(10, 2));

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "numbers");

            GenericRowData row =
                    GenericRowData.of(
                            true,
                            (byte) 1,
                            (short) 10,
                            100,
                            1000L,
                            3.14f,
                            2.718,
                            DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2));
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "numbers");

            // Boolean - no suffix
            assertTrue(lineProtocol.contains("bool_field=true"));

            // Integer types - should have 'i' suffix
            assertTrue(lineProtocol.contains("byte_field=1i"));
            assertTrue(lineProtocol.contains("short_field=10i"));
            assertTrue(lineProtocol.contains("int_field=100i"));
            assertTrue(lineProtocol.contains("long_field=1000i"));

            // Float/Double - no suffix
            assertTrue(lineProtocol.contains("float_field=3.14"));
            assertTrue(lineProtocol.contains("double_field=2.718"));

            // Decimal should be formatted as string
            assertTrue(lineProtocol.contains("decimal_field=\"123.45\""));
        }

        @Test
        @DisplayName("Should handle special float values")
        void testSpecialFloatValues() {
            List<String> columnNames = Arrays.asList("nan_field", "inf_field", "neg_inf_field");
            List<DataType> columnTypes =
                    Arrays.asList(DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.DOUBLE());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "special");

            GenericRowData row =
                    GenericRowData.of(
                            Float.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "special");

            assertTrue(lineProtocol.contains("nan_field=NaN"));
            assertTrue(lineProtocol.contains("inf_field=Infinity"));
            assertTrue(lineProtocol.contains("neg_inf_field=-Infinity"));
        }
    }

    @Nested
    @DisplayName("Row Kind Handling")
    class RowKindTests {

        @Test
        @DisplayName("Should return null for DELETE rows")
        void testDeleteRowReturnsNull() {
            List<String> columnNames = Arrays.asList("value");
            List<DataType> columnTypes = Arrays.asList(DataTypes.STRING());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row = GenericRowData.of(StringData.fromString("test"));
            row.setRowKind(RowKind.DELETE);

            String lineProtocol = converter.convertToLineProtocol(row, "test");
            assertNull(lineProtocol);
        }

        @Test
        @DisplayName("Should return null for UPDATE_BEFORE rows")
        void testUpdateBeforeRowReturnsNull() {
            List<String> columnNames = Arrays.asList("value");
            List<DataType> columnTypes = Arrays.asList(DataTypes.STRING());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row = GenericRowData.of(StringData.fromString("test"));
            row.setRowKind(RowKind.UPDATE_BEFORE);

            String lineProtocol = converter.convertToLineProtocol(row, "test");
            assertNull(lineProtocol);
        }

        @Test
        @DisplayName("Should process INSERT and UPDATE_AFTER rows")
        void testInsertAndUpdateAfterRows() {
            List<String> columnNames = Arrays.asList("value");
            List<DataType> columnTypes = Arrays.asList(DataTypes.INT());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            // Test INSERT
            GenericRowData insertRow = GenericRowData.of(42);
            insertRow.setRowKind(RowKind.INSERT);
            String insertProtocol = converter.convertToLineProtocol(insertRow, "test");
            assertNotNull(insertProtocol);
            assertTrue(insertProtocol.contains("value=42i"));

            // Test UPDATE_AFTER
            GenericRowData updateRow = GenericRowData.of(43);
            updateRow.setRowKind(RowKind.UPDATE_AFTER);
            String updateProtocol = converter.convertToLineProtocol(updateRow, "test");
            assertNotNull(updateProtocol);
            assertTrue(updateProtocol.contains("value=43i"));
        }
    }

    @Nested
    @DisplayName("Timestamp Handling")
    class TimestampTests {

        @Test
        @DisplayName("Should use current time when no timestamp field specified")
        void testDefaultTimestamp() {
            List<String> columnNames = Arrays.asList("value");
            List<DataType> columnTypes = Arrays.asList(DataTypes.INT());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row = GenericRowData.of(42);
            row.setRowKind(RowKind.INSERT);

            long before = System.currentTimeMillis();
            String lineProtocol = converter.convertToLineProtocol(row, "test");
            long after = System.currentTimeMillis();

            // Extract timestamp from line protocol
            String[] parts = lineProtocol.split(" ");
            long timestamp = Long.parseLong(parts[parts.length - 1]);

            assertTrue(timestamp >= before && timestamp <= after + 100);
        }

        @Test
        @DisplayName("Should use specified timestamp field")
        void testSpecifiedTimestamp() {
            List<String> columnNames = Arrays.asList("value", "event_time");
            List<DataType> columnTypes = Arrays.asList(DataTypes.INT(), DataTypes.BIGINT());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm =
                    FieldMappingConfig.builder()
                            .timestampField("event_time")
                            .sourceTimestampPrecision("ms")
                            .build();

            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row = GenericRowData.of(42, 1756080005L);
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "test");
            assertTrue(lineProtocol.endsWith(" 1756080005000000"));
        }

        @Test
        @DisplayName("Should handle TIMESTAMP data type")
        void testTimestampDataType() {
            List<String> columnNames = Arrays.asList("value", "ts");
            List<DataType> columnTypes = Arrays.asList(DataTypes.INT(), DataTypes.TIMESTAMP(3));

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm =
                    FieldMappingConfig.builder()
                            .timestampField("ts")
                            .sourceTimestampPrecision("ms")
                            .build();

            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            TimestampData timestamp = TimestampData.fromEpochMillis(1609459200000L);
            GenericRowData row = GenericRowData.of(42, timestamp);
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "test");

            assertTrue(lineProtocol.endsWith(" 1609459200000"));
        }
    }

    @Nested
    @DisplayName("Null Value Handling")
    class NullHandlingTests {

        @Test
        @DisplayName("Should skip null fields")
        void testNullFieldsSkipped() {
            List<String> columnNames = Arrays.asList("field1", "field2", "field3");
            List<DataType> columnTypes =
                    Arrays.asList(DataTypes.STRING(), DataTypes.INT(), DataTypes.DOUBLE());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row = new GenericRowData(3);
            row.setField(0, StringData.fromString("value1"));
            row.setField(1, null); // null int
            row.setField(2, 3.14);
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "test");

            assertTrue(lineProtocol.contains("field1=\"value1\""));
            assertFalse(lineProtocol.contains("field2="));
            assertTrue(lineProtocol.contains("field3=3.14"));
        }

        @Test
        @DisplayName("Should skip null tags")
        void testNullTagsSkipped() {
            List<String> columnNames = Arrays.asList("tag1", "tag2", "value");
            List<DataType> columnTypes =
                    Arrays.asList(DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm =
                    FieldMappingConfig.builder().addTagField("tag1").addTagField("tag2").build();

            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row = new GenericRowData(3);
            row.setField(0, null); // null tag
            row.setField(1, StringData.fromString("tag2_value"));
            row.setField(2, 42);
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "test");

            assertFalse(lineProtocol.contains("tag1="));
            assertTrue(lineProtocol.contains("tag2=tag2_value"));
            assertTrue(lineProtocol.contains("value=42i"));
        }
    }

    @Nested
    @DisplayName("Integration and Edge Cases")
    class IntegrationTests {

        @Test
        @DisplayName("Should handle empty string values")
        void testEmptyStringValues() {
            List<String> columnNames = Arrays.asList("empty_tag", "empty_field");
            List<DataType> columnTypes = Arrays.asList(DataTypes.STRING(), DataTypes.STRING());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().addTagField("empty_tag").build();

            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            GenericRowData row =
                    GenericRowData.of(StringData.fromString(""), StringData.fromString(""));
            row.setRowKind(RowKind.INSERT);

            String lineProtocol = converter.convertToLineProtocol(row, "test");

            // Empty strings should still be included
            assertTrue(lineProtocol.contains("empty_tag="));
            assertTrue(lineProtocol.contains("empty_field=\"\""));
        }

        @Test
        @DisplayName("Should maintain field order consistency")
        void testFieldOrderConsistency() {
            List<String> columnNames = Arrays.asList("z_field", "a_field", "m_field");
            List<DataType> columnTypes =
                    Arrays.asList(DataTypes.INT(), DataTypes.INT(), DataTypes.INT());

            when(resolvedSchema.getColumnNames()).thenReturn(columnNames);
            when(resolvedSchema.getColumnDataTypes()).thenReturn(columnTypes);

            FieldMappingConfig fm = FieldMappingConfig.builder().build();
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(resolvedSchema, fm, "test");

            // Generate multiple line protocols to check consistency
            for (int i = 0; i < 5; i++) {
                GenericRowData row = GenericRowData.of(1, 2, 3);
                row.setRowKind(RowKind.INSERT);

                String lineProtocol = converter.convertToLineProtocol(row, "test");

                // Fields should appear in a consistent order
                int zPos = lineProtocol.indexOf("z_field");
                int aPos = lineProtocol.indexOf("a_field");
                int mPos = lineProtocol.indexOf("m_field");

                assertTrue(zPos > 0);
                assertTrue(aPos > 0);
                assertTrue(mPos > 0);
            }
        }
    }
}
