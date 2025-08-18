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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.util.function.SerializableFunction;

import io.opengemini.client.api.Point;

/**
 * A builder-style converter for creating OpenGemini Points. This version uses Flink's built-in
 * SerializableFunction.
 *
 * @param <T> The type of object to convert
 */
public class SimpleOpenGeminiPointConverter<T> implements OpenGeminiPointConverter<T> {
    private static final long serialVersionUID = 1L;

    private final Map<String, SerializableFunction<T, String>> tagExtractors;
    private final Map<String, SerializableFunction<T, Object>> fieldExtractors;
    private final SerializableFunction<T, Long> timestampExtractor;

    private SimpleOpenGeminiPointConverter(
            Map<String, SerializableFunction<T, String>> tagExtractors,
            Map<String, SerializableFunction<T, Object>> fieldExtractors,
            SerializableFunction<T, Long> timestampExtractor) {
        // Create defensive copies to ensure immutability
        this.tagExtractors = new HashMap<>(tagExtractors);
        this.fieldExtractors = new HashMap<>(fieldExtractors);
        this.timestampExtractor = timestampExtractor;
    }

    /**
     * Converts the given value into an OpenGemini `Point` object with the specified measurement
     * name. The method extracts tags, fields, and a timestamp from the value using the configured
     * extractors.
     *
     * @param value The input value to convert. If null, the method returns null.
     * @param measurement The name of the measurement to set in the `Point`.
     * @return A `Point` object representing the converted value, or null if the value is null or no
     *     fields are extracted.
     * @throws RuntimeException If an error occurs while extracting tags, fields, or the timestamp.
     */
    @Override
    public Point convert(T value, String measurement) {
        if (value == null) {
            return null;
        }

        Point point = new Point();
        point.setMeasurement(measurement);

        // Set tags
        Map<String, String> tags = new HashMap<>();
        for (Map.Entry<String, SerializableFunction<T, String>> entry : tagExtractors.entrySet()) {
            try {
                String tagValue = entry.getValue().apply(value);
                if (tagValue != null) {
                    tags.put(entry.getKey(), tagValue);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error extracting tag " + entry.getKey(), e);
            }
        }
        if (!tags.isEmpty()) {
            point.setTags(tags);
        }

        // Set fields
        Map<String, Object> fields = new HashMap<>();
        for (Map.Entry<String, SerializableFunction<T, Object>> entry :
                fieldExtractors.entrySet()) {
            try {
                Object fieldValue = entry.getValue().apply(value);
                if (fieldValue != null) {
                    fields.put(entry.getKey(), fieldValue);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error extracting field " + entry.getKey(), e);
            }
        }
        if (fields.isEmpty()) {
            // At least one field is required
            return null;
        }
        point.setFields(fields);

        // Set timestamp
        if (timestampExtractor != null) {
            try {
                Long timestamp = timestampExtractor.apply(value);
                if (timestamp != null) {
                    point.setTime(timestamp);
                } else {
                    point.setTime(
                            System.currentTimeMillis() * 1_000_000L); // Current time in nanoseconds
                }
            } catch (Exception e) {
                throw new RuntimeException("Error extracting timestamp", e);
            }
        } else {
            point.setTime(System.currentTimeMillis() * 1_000_000L);
        }

        return point;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private final Map<String, SerializableFunction<T, String>> tagExtractors = new HashMap<>();
        private final Map<String, SerializableFunction<T, Object>> fieldExtractors =
                new HashMap<>();
        private SerializableFunction<T, Long> timestampExtractor;

        public Builder<T> addTag(String tagName, SerializableFunction<T, String> extractor) {
            tagExtractors.put(tagName, extractor);
            return this;
        }

        public Builder<T> addField(String fieldName, SerializableFunction<T, Object> extractor) {
            fieldExtractors.put(fieldName, extractor);
            return this;
        }

        public Builder<T> withTimestamp(SerializableFunction<T, Long> extractor) {
            this.timestampExtractor = extractor;
            return this;
        }

        public Builder<T> withTimestampMillis(SerializableFunction<T, Long> extractor) {
            // Convert milliseconds to nanoseconds
            this.timestampExtractor =
                    value -> {
                        try {
                            Long millis = extractor.apply(value);
                            return millis != null ? millis * 1_000_000L : null;
                        } catch (Exception e) {
                            throw new RuntimeException("Error extracting timestamp", e);
                        }
                    };
            return this;
        }

        /**
         * Configures the converter to extract a timestamp from the given value as an `Instant`. The
         * extracted `Instant` is converted to nanoseconds since the epoch.
         *
         * @param extractor A `SerializableFunction` that extracts an `Instant` from the input
         *     value.
         * @return The `Builder` instance for method chaining.
         * @throws RuntimeException If an error occurs while extracting the timestamp as an
         *     `Instant`.
         */
        public Builder<T> withTimestampInstant(SerializableFunction<T, Instant> extractor) {
            // Convert Instant to nanoseconds
            this.timestampExtractor =
                    value -> {
                        try {
                            Instant instant = extractor.apply(value);
                            return instant != null ? instant.toEpochMilli() * 1_000_000L : null;
                        } catch (Exception e) {
                            throw new RuntimeException("Error extracting timestamp instant", e);
                        }
                    };
            return this;
        }

        public SimpleOpenGeminiPointConverter<T> build() {
            if (fieldExtractors.isEmpty()) {
                throw new IllegalStateException("At least one field must be defined");
            }
            return new SimpleOpenGeminiPointConverter<>(
                    tagExtractors, fieldExtractors, timestampExtractor);
        }
    }
}
