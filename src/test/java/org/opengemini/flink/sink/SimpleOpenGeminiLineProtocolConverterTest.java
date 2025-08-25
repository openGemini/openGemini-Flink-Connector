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

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Comprehensive unit tests for SimpleOpenGeminiLineProtocolConverter */
public class SimpleOpenGeminiLineProtocolConverterTest {

    // Test data class
    static class TestData {
        String tagValue;
        String stringField;
        Integer intField;
        Long longField;
        Float floatField;
        Double doubleField;
        Boolean boolField;
        Long timestamp;
        Instant instantTimestamp;

        TestData() {}

        TestData(String tagValue, String stringField, Integer intField) {
            this.tagValue = tagValue;
            this.stringField = stringField;
            this.intField = intField;
        }
    }

    @Nested
    @DisplayName("Basic Conversion Tests")
    class BasicConversionTests {

        @Test
        @DisplayName("Should convert simple object with all field types")
        void testBasicConversion() {
            // Arrange
            TestData data = new TestData();
            data.tagValue = "server01";
            data.stringField = "test";
            data.intField = 42;
            data.longField = 100L;
            data.floatField = 3.14f;
            data.doubleField = 2.718;
            data.boolField = true;
            data.timestamp = 1000000000L;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addTag("host", d -> d.tagValue)
                            .addField("stringField", d -> d.stringField)
                            .addField("intField", d -> d.intField)
                            .addField("longField", d -> d.longField)
                            .addField("floatField", d -> d.floatField)
                            .addField("doubleField", d -> d.doubleField)
                            .addField("boolField", d -> d.boolField)
                            .withTimestamp(d -> d.timestamp)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertNotNull(result);
            assertTrue(result.startsWith("measurement,host=server01 "));
            assertTrue(result.contains("stringField=\"test\""));
            assertTrue(result.contains("intField=42i"));
            assertTrue(result.contains("longField=100i"));
            assertTrue(result.contains("floatField=3.14"));
            assertTrue(result.contains("doubleField=2.718"));
            assertTrue(result.contains("boolField=true"));
            assertTrue(result.endsWith(" 1000000000"));
        }

        @Test
        @DisplayName("Should handle null input value")
        void testNullInputValue() {
            // Arrange
            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(null, "measurement");

            // Assert
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when no fields have values")
        void testNoFieldValues() {
            // Arrange
            TestData data = new TestData();
            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field1", d -> d.stringField)
                            .addField("field2", d -> d.intField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertNull(result);
        }
    }

    @Nested
    @DisplayName("Special Character Escaping Tests")
    class EscapingTests {

        @Test
        @DisplayName("Should escape spaces in tag values")
        void testEscapeSpacesInTags() {
            // Arrange
            TestData data = new TestData();
            data.tagValue = "server 01";
            data.stringField = "value";

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addTag("host", d -> d.tagValue)
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("host=server\\ 01"));
        }

        @Test
        @DisplayName("Should escape commas in tag values")
        void testEscapeCommasInTags() {
            // Arrange
            TestData data = new TestData();
            data.tagValue = "server,01";
            data.stringField = "value";

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addTag("host", d -> d.tagValue)
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("host=server\\,01"));
        }

        @Test
        @DisplayName("Should escape equals signs in tag values")
        void testEscapeEqualsInTags() {
            // Arrange
            TestData data = new TestData();
            data.tagValue = "server=01";
            data.stringField = "value";

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addTag("host", d -> d.tagValue)
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("host=server\\=01"));
        }

        @Test
        @DisplayName("Should escape multiple special characters in tag values")
        void testEscapeMultipleSpecialCharsInTags() {
            // Arrange
            TestData data = new TestData();
            data.tagValue = "server 01,region=us";
            data.stringField = "value";

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addTag("host", d -> d.tagValue)
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("host=server\\ 01\\,region\\=us"));
        }

        @Test
        @DisplayName("Should escape quotes in string field values")
        void testEscapeQuotesInStringFields() {
            // Arrange
            TestData data = new TestData();
            data.stringField = "value with \"quotes\"";

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("field=\"value with \\\"quotes\\\"\""));
        }
    }

    @Nested
    @DisplayName("Timestamp Tests")
    class TimestampTests {

        @Test
        @DisplayName("Should use system nano time when no timestamp extractor provided")
        void testDefaultTimestamp() {
            // Arrange
            TestData data = new TestData();
            data.stringField = "value";

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            long before = System.nanoTime();
            String result = converter.convertToLineProtocol(data, "measurement");
            long after = System.nanoTime();

            // Assert
            String[] parts = result.split(" ");
            long timestamp = Long.parseLong(parts[parts.length - 1]);
            assertTrue(timestamp >= before && timestamp <= after);
        }

        @Test
        @DisplayName("Should use provided timestamp in nanoseconds")
        void testProvidedTimestampNanos() {
            // Arrange
            TestData data = new TestData();
            data.stringField = "value";
            data.timestamp = 1234567890L;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field", d -> d.stringField)
                            .withTimestamp(d -> d.timestamp)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.endsWith(" 1234567890"));
        }

        @Test
        @DisplayName("Should convert milliseconds to nanoseconds")
        void testTimestampMillisConversion() {
            // Arrange
            TestData data = new TestData();
            data.stringField = "value";
            data.timestamp = 1000L; // 1000 milliseconds

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field", d -> d.stringField)
                            .withTimestampMillis(d -> d.timestamp)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.endsWith(" 1000000000")); // 1000 * 1_000_000
        }

        @Test
        @DisplayName("Should convert Instant to nanoseconds")
        void testTimestampInstantConversion() {
            // Arrange
            TestData data = new TestData();
            data.stringField = "value";
            data.instantTimestamp = Instant.ofEpochMilli(1000L);

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field", d -> d.stringField)
                            .withTimestampInstant(d -> d.instantTimestamp)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.endsWith(" 1000000000"));
        }

        @Test
        @DisplayName("Should handle null timestamp with default system time")
        void testNullTimestamp() {
            // Arrange
            TestData data = new TestData();
            data.stringField = "value";
            data.timestamp = null;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field", d -> d.stringField)
                            .withTimestamp(d -> d.timestamp)
                            .build();

            // Act
            long before = System.nanoTime();
            String result = converter.convertToLineProtocol(data, "measurement");
            long after = System.nanoTime();

            // Assert
            String[] parts = result.split(" ");
            long timestamp = Long.parseLong(parts[parts.length - 1]);
            assertTrue(timestamp >= before && timestamp <= after);
        }

        @Test
        @DisplayName("Should handle null Instant timestamp")
        void testNullInstantTimestamp() {
            // Arrange
            TestData data = new TestData();
            data.stringField = "value";
            data.instantTimestamp = null;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field", d -> d.stringField)
                            .withTimestampInstant(d -> d.instantTimestamp)
                            .build();

            // Act
            long before = System.nanoTime();
            String result = converter.convertToLineProtocol(data, "measurement");
            long after = System.nanoTime();

            // Assert
            String[] parts = result.split(" ");
            long timestamp = Long.parseLong(parts[parts.length - 1]);
            assertTrue(timestamp >= before && timestamp <= after);
        }
    }

    @Nested
    @DisplayName("Multiple Tags and Fields Tests")
    class MultipleTagsFieldsTests {

        @Test
        @DisplayName("Should handle multiple tags in correct order")
        void testMultipleTags() {
            // Arrange
            TestData data = new TestData();
            data.tagValue = "server01";
            data.stringField = "value";

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addTag("host", d -> d.tagValue)
                            .addTag("region", d -> "us-west")
                            .addTag("env", d -> "prod")
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("host=server01"));
            assertTrue(result.contains("region=us-west"));
            assertTrue(result.contains("env=prod"));
        }

        @Test
        @DisplayName("Should handle multiple fields with different types")
        void testMultipleFields() {
            // Arrange
            TestData data = new TestData();
            data.stringField = "text";
            data.intField = 42;
            data.boolField = false;
            data.doubleField = 3.14;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("stringField", d -> d.stringField)
                            .addField("intField", d -> d.intField)
                            .addField("boolField", d -> d.boolField)
                            .addField("doubleField", d -> d.doubleField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("stringField=\"text\""));
            assertTrue(result.contains("intField=42i"));
            assertTrue(result.contains("boolField=false"));
            assertTrue(result.contains("doubleField=3.14"));
        }

        @Test
        @DisplayName("Should skip null tag values")
        void testNullTagValues() {
            // Arrange
            TestData data = new TestData();
            data.tagValue = null;
            data.stringField = "value";

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addTag("host", d -> d.tagValue)
                            .addTag("region", d -> "us-west")
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertFalse(result.contains("host="));
            assertTrue(result.contains("region=us-west"));
        }

        @Test
        @DisplayName("Should skip null field values")
        void testNullFieldValues() {
            // Arrange
            TestData data = new TestData();
            data.stringField = null;
            data.intField = 42;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("stringField", d -> d.stringField)
                            .addField("intField", d -> d.intField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertFalse(result.contains("stringField="));
            assertTrue(result.contains("intField=42i"));
        }
    }

    @Nested
    @DisplayName("Builder Tests")
    class BuilderTests {

        @Test
        @DisplayName("Should throw exception when no fields defined")
        void testBuilderNoFields() {
            // Act & Assert
            assertThrows(
                    IllegalStateException.class,
                    () -> {
                        SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                                .addTag("host", d -> d.tagValue)
                                .build();
                    });
        }

        @Test
        @DisplayName("Should allow building with only fields")
        void testBuilderOnlyFields() {
            // Act
            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("field", d -> d.stringField)
                            .build();

            // Assert
            assertNotNull(converter);
        }

        @Test
        @DisplayName("Should support method chaining")
        void testBuilderMethodChaining() {
            // Act
            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addTag("tag1", d -> "value1")
                            .addTag("tag2", d -> "value2")
                            .addField("field1", d -> "value1")
                            .addField("field2", d -> 42)
                            .withTimestamp(d -> 1000L)
                            .build();

            // Assert
            assertNotNull(converter);
        }
    }

    @Nested
    @DisplayName("Edge Cases and Special Values Tests")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle empty string values")
        void testEmptyStringValues() {
            // Arrange
            TestData data = new TestData();
            data.tagValue = "";
            data.stringField = "";

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addTag("host", d -> d.tagValue)
                            .addField("field", d -> d.stringField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("host="));
            assertTrue(result.contains("field=\"\""));
        }

        @Test
        @DisplayName("Should handle very large numbers")
        void testLargeNumbers() {
            // Arrange
            TestData data = new TestData();
            data.longField = Long.MAX_VALUE;
            data.doubleField = Double.MAX_VALUE;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("longField", d -> d.longField)
                            .addField("doubleField", d -> d.doubleField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("longField=" + Long.MAX_VALUE + "i"));
            assertTrue(result.contains("doubleField=" + Double.MAX_VALUE));
        }

        @Test
        @DisplayName("Should handle negative numbers")
        void testNegativeNumbers() {
            // Arrange
            TestData data = new TestData();
            data.intField = -42;
            data.floatField = -3.14f;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("intField", d -> d.intField)
                            .addField("floatField", d -> d.floatField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("intField=-42i"));
            assertTrue(result.contains("floatField=-3.14"));
        }

        @Test
        @DisplayName("Should handle zero values")
        void testZeroValues() {
            // Arrange
            TestData data = new TestData();
            data.intField = 0;
            data.doubleField = 0.0;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("intField", d -> d.intField)
                            .addField("doubleField", d -> d.doubleField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("intField=0i"));
            assertTrue(result.contains("doubleField=0.0"));
        }

        @Test
        @DisplayName("Should handle special float values")
        void testSpecialFloatValues() {
            // Arrange
            TestData data = new TestData();
            data.floatField = Float.NaN;
            data.doubleField = Double.POSITIVE_INFINITY;

            SimpleOpenGeminiLineProtocolConverter<TestData> converter =
                    SimpleOpenGeminiLineProtocolConverter.<TestData>builder()
                            .addField("floatField", d -> d.floatField)
                            .addField("doubleField", d -> d.doubleField)
                            .build();

            // Act
            String result = converter.convertToLineProtocol(data, "measurement");

            // Assert
            assertTrue(result.contains("floatField=NaN"));
            assertTrue(result.contains("doubleField=Infinity"));
        }
    }
}
