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
import java.util.StringJoiner;

import org.apache.flink.util.function.SerializableFunction;

/**
 * A builder-style converter for creating Line Protocol strings directly. This is the
 * high-performance alternative to SimpleOpenGeminiPointConverter.
 *
 * @param <T> The type of object to convert
 */
public class SimpleOpenGeminiLineProtocolConverter<T>
        implements OpenGeminiLineProtocolConverter<T> {
    private static final long serialVersionUID = 1L;

    private final Map<String, SerializableFunction<T, String>> tagExtractors;
    private final Map<String, SerializableFunction<T, Object>> fieldExtractors;
    private final SerializableFunction<T, Long> timestampExtractor;

    private SimpleOpenGeminiLineProtocolConverter(
            Map<String, SerializableFunction<T, String>> tagExtractors,
            Map<String, SerializableFunction<T, Object>> fieldExtractors,
            SerializableFunction<T, Long> timestampExtractor) {
        this.tagExtractors = new HashMap<>(tagExtractors);
        this.fieldExtractors = new HashMap<>(fieldExtractors);
        this.timestampExtractor = timestampExtractor;
    }

    @Override
    public String convertToLineProtocol(T value, String measurement) {
        if (value == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(measurement);

        // Tags
        for (Map.Entry<String, SerializableFunction<T, String>> entry : tagExtractors.entrySet()) {
            String tagValue = entry.getValue().apply(value);
            if (tagValue != null) {
                sb.append(',').append(entry.getKey()).append('=').append(escape(tagValue));
            }
        }

        // Fields
        sb.append(' ');
        StringJoiner fieldJoiner = new StringJoiner(",");
        for (Map.Entry<String, SerializableFunction<T, Object>> entry :
                fieldExtractors.entrySet()) {
            Object fieldValue = entry.getValue().apply(value);
            if (fieldValue != null) {
                fieldJoiner.add(formatField(entry.getKey(), fieldValue));
            }
        }

        if (fieldJoiner.length() == 0) {
            return null; // At least one field required
        }
        sb.append(fieldJoiner.toString());

        // Timestamp
        if (timestampExtractor != null) {
            Long timestamp = timestampExtractor.apply(value);
            sb.append(' ').append(timestamp != null ? timestamp : System.nanoTime());
        } else {
            sb.append(' ').append(System.nanoTime());
        }

        return sb.toString();
    }

    public static String escape(String value) {
        return value.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=");
    }

    public static String formatField(String name, Object value) {
        if (value instanceof String) {
            return name + "=\"" + ((String) value).replace("\"", "\\\"") + "\"";
        } else if (value instanceof Boolean) {
            return name + "=" + value;
        } else if (value instanceof Number) {
            if (value instanceof Float || value instanceof Double) {
                return name + "=" + value;
            } else {
                return name + "=" + value + "i";
            }
        }
        return name + "=\"" + value.toString() + "\"";
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private final Map<String, SerializableFunction<T, String>> tagExtractors = new HashMap<>();
        private final Map<String, SerializableFunction<T, java.lang.Object>> fieldExtractors =
                new HashMap<>();
        private SerializableFunction<T, Long> timestampExtractor;

        public SimpleOpenGeminiLineProtocolConverter.Builder<T> addTag(
                String tagName, SerializableFunction<T, String> extractor) {
            tagExtractors.put(tagName, extractor);
            return this;
        }

        public SimpleOpenGeminiLineProtocolConverter.Builder<T> addField(
                String fieldName, SerializableFunction<T, java.lang.Object> extractor) {
            fieldExtractors.put(fieldName, extractor);
            return this;
        }

        public SimpleOpenGeminiLineProtocolConverter.Builder<T> withTimestamp(
                SerializableFunction<T, Long> extractor) {
            this.timestampExtractor = extractor;
            return this;
        }

        public SimpleOpenGeminiLineProtocolConverter.Builder<T> withTimestampMillis(
                SerializableFunction<T, Long> extractor) {
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
        public SimpleOpenGeminiLineProtocolConverter.Builder<T> withTimestampInstant(
                SerializableFunction<T, Instant> extractor) {
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

        public SimpleOpenGeminiLineProtocolConverter<T> build() {
            if (fieldExtractors.isEmpty()) {
                throw new IllegalStateException("At least one field must be defined");
            }
            return new SimpleOpenGeminiLineProtocolConverter<>(
                    tagExtractors, fieldExtractors, timestampExtractor);
        }
    }
}
