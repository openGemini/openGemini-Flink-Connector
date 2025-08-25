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
import java.util.Collections;
import java.util.Map;

import org.apache.flink.util.function.SerializableFunction;
import org.junit.jupiter.api.Test;

import io.opengemini.client.api.Point;

class SimpleOpenGeminiPointConverterTest {

    static class TestData {
        String tag;
        Integer field;
        Long timestampMillis;
        Instant timestampInstant;

        TestData(String tag, Integer field, Long timestampMillis, Instant timestampInstant) {
            this.tag = tag;
            this.field = field;
            this.timestampMillis = timestampMillis;
            this.timestampInstant = timestampInstant;
        }
    }

    @Test
    void testConvertNullReturnsNull() {
        SimpleOpenGeminiPointConverter<TestData> converter =
                SimpleOpenGeminiPointConverter.<TestData>builder()
                        .addField("f", d -> d.field)
                        .build();
        assertNull(converter.convertToPoint(null, "m"));
    }

    @Test
    void testBuildWithoutFieldsThrows() {
        SimpleOpenGeminiPointConverter.Builder<TestData> builder =
                SimpleOpenGeminiPointConverter.builder();
        IllegalStateException ex = assertThrows(IllegalStateException.class, builder::build);
        assertEquals("At least one field must be defined", ex.getMessage());
    }

    @Test
    void testConvertWithTagAndField() {
        TestData t = new TestData("t1", 42, null, null);
        SimpleOpenGeminiPointConverter<TestData> converter =
                SimpleOpenGeminiPointConverter.<TestData>builder()
                        .addTag("tag1", dd -> dd.tag)
                        .addField("field1", dd -> dd.field)
                        .build();

        Point p = converter.convertToPoint(t, "measurement1");
        assertNotNull(p);
        assertEquals("measurement1", p.getMeasurement());
        Map<String, String> tags = p.getTags();
        assertEquals(1, tags.size());
        assertEquals("t1", tags.get("tag1"));
        Map<String, Object> fields = p.getFields();
        assertEquals(1, fields.size());
        assertEquals(42, fields.get("field1"));
        assertTrue(p.getTime() > 0);
    }

    @Test
    void testConvertWithoutTags() {
        TestData t = new TestData(null, 100, null, null);
        SimpleOpenGeminiPointConverter<TestData> converter =
                SimpleOpenGeminiPointConverter.<TestData>builder()
                        .addField("f", dd -> dd.field)
                        .build();

        Point p = converter.convertToPoint(t, "m");
        assertNotNull(p);
        assertNull(p.getTags());
        assertEquals(Collections.singletonMap("f", 100), p.getFields());
    }

    @Test
    void testWithTimestampMillis() {
        TestData t = new TestData(null, 1, 12345L, null);
        SimpleOpenGeminiPointConverter<TestData> converter =
                SimpleOpenGeminiPointConverter.<TestData>builder()
                        .addField("f", dd -> dd.field)
                        .withTimestampMillis(dd -> dd.timestampMillis)
                        .build();

        Point p = converter.convertToPoint(t, "m");
        assertEquals(12345L * 1_000_000L, p.getTime());
    }

    @Test
    void testWithTimestampInstant() {
        Instant now = Instant.ofEpochMilli(9999);
        TestData t = new TestData(null, 2, null, now);
        SimpleOpenGeminiPointConverter<TestData> converter =
                SimpleOpenGeminiPointConverter.<TestData>builder()
                        .addField("f", dd -> dd.field)
                        .withTimestampInstant(dd -> dd.timestampInstant)
                        .build();

        Point p = converter.convertToPoint(t, "m");
        assertEquals(9999L * 1_000_000L, p.getTime());
    }

    @Test
    void testTagExtractorThrows() {
        TestData t = new TestData("bad", 5, null, null);
        SerializableFunction<TestData, String> badTag =
                dd -> {
                    throw new RuntimeException("boom");
                };
        SimpleOpenGeminiPointConverter<TestData> converter =
                SimpleOpenGeminiPointConverter.<TestData>builder()
                        .addTag("badTag", badTag)
                        .addField("f", dd -> dd.field)
                        .build();

        RuntimeException ex =
                assertThrows(RuntimeException.class, () -> converter.convertToPoint(t, "m"));
        assertTrue(ex.getMessage().contains("Error extracting tag badTag"));
    }

    @Test
    void testFieldExtractorThrows() {
        TestData t = new TestData("t", 0, null, null);
        SerializableFunction<TestData, Object> badField =
                dd -> {
                    throw new IllegalStateException("fail");
                };
        SimpleOpenGeminiPointConverter<TestData> converter =
                SimpleOpenGeminiPointConverter.<TestData>builder()
                        .addField("badField", badField)
                        .build();

        RuntimeException ex =
                assertThrows(RuntimeException.class, () -> converter.convertToPoint(t, "m"));
        assertTrue(ex.getMessage().contains("Error extracting field badField"));
    }

    @Test
    void testConvertNoFieldsResultsNull() {
        TestData t = new TestData("t", null, null, null);
        SimpleOpenGeminiPointConverter<TestData> converter =
                SimpleOpenGeminiPointConverter.<TestData>builder()
                        .addField("f", dd -> null)
                        .build();

        assertNull(converter.convertToPoint(t, "m"));
    }
}
