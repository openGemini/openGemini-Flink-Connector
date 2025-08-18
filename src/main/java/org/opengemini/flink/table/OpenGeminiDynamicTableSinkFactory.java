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

import java.io.Serializable;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.opengemini.flink.sink.OpenGeminiPointConverter;
import org.opengemini.flink.sink.OpenGeminiSinkConfiguration;

import io.opengemini.client.api.Point;

/** Factory for creating OpenGemini table sink in Table API/SQL. */
public class OpenGeminiDynamicTableSinkFactory implements DynamicTableSinkFactory {

    // Connector identifier used in 'connector' = 'opengemini'
    public static final String IDENTIFIER = "opengemini";
    public static final String TAG_FIELD_DELIMITER = ",";

    // Required options
    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OpenGemini server host");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(8086)
                    .withDescription("OpenGemini server port");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OpenGemini database name");

    public static final ConfigOption<String> MEASUREMENT =
            ConfigOptions.key("measurement")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OpenGemini measurement name");

    // Optional authentication options
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OpenGemini username for authentication");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OpenGemini password for authentication");

    // Optional performance tuning options
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of points to batch before writing");

    public static final ConfigOption<Duration> FLUSH_INTERVAL =
            ConfigOptions.key("flush-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Maximum time to wait before flushing a batch");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum number of retries for failed writes");

    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
            ConfigOptions.key("connection-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("Connection timeout");

    public static final ConfigOption<Duration> REQUEST_TIMEOUT =
            ConfigOptions.key("request-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("Request timeout for write operations");

    // Field mapping options
    public static final ConfigOption<String> TIMESTAMP_FIELD =
            ConfigOptions.key("timestamp-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Field name to use as timestamp (optional, uses processing time if not set)");

    public static final ConfigOption<String> TAG_FIELDS =
            ConfigOptions.key("tag-fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Comma-separated list of fields to use as tags");

    public static final ConfigOption<String> FIELD_FIELDS =
            ConfigOptions.key("field-fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated list of fields to use as fields (default: all non-tag fields)");

    // Write behavior options
    public static final ConfigOption<Boolean> IGNORE_NULL_VALUES =
            ConfigOptions.key("ignore-null-values")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to ignore null values when writing");

    public static final ConfigOption<String> WRITE_PRECISION =
            ConfigOptions.key("write-precision")
                    .stringType()
                    .defaultValue("ms")
                    .withDescription("Timestamp precision: ns, us, ms, s, m, h");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> required = new HashSet<>();
        required.add(HOST);
        required.add(DATABASE);
        required.add(MEASUREMENT);
        return required;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optional = new HashSet<>();

        // Connection options
        optional.add(PORT);
        optional.add(USERNAME);
        optional.add(PASSWORD);
        optional.add(CONNECTION_TIMEOUT);
        optional.add(REQUEST_TIMEOUT);

        // Performance options
        optional.add(BATCH_SIZE);
        optional.add(FLUSH_INTERVAL);
        optional.add(MAX_RETRIES);

        // Field mapping options
        optional.add(TIMESTAMP_FIELD);
        optional.add(TAG_FIELDS);
        optional.add(FIELD_FIELDS);

        // Write behavior options
        optional.add(IGNORE_NULL_VALUES);
        optional.add(WRITE_PRECISION);

        // Add standard Flink options
        optional.add(FactoryUtil.SINK_PARALLELISM);

        return optional;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig config = helper.getOptions();

        // Build configuration - THE PROBLEM IS HERE!
        OpenGeminiSinkConfiguration.Builder<RowData> configBuilder =
                OpenGeminiSinkConfiguration.<RowData>builder()
                        .host(config.get(HOST))
                        .port(config.get(PORT))
                        .database(config.get(DATABASE))
                        .measurement(config.get(MEASUREMENT))
                        .batchSize(config.get(BATCH_SIZE))
                        .flushIntervalMillis(config.get(FLUSH_INTERVAL).toMillis())
                        .maxRetries(config.get(MAX_RETRIES))
                        .connectionTimeout(config.get(CONNECTION_TIMEOUT))
                        .requestTimeout(config.get(REQUEST_TIMEOUT));

        // Set authentication
        String username = config.getOptional(USERNAME).orElse(null);
        String password = config.getOptional(PASSWORD).orElse(null);
        if (username != null && password != null) {
            configBuilder.username(username).password(password);
        }

        // TODO: this problem is caused by coupling of config and converter. will be fixed
        // Create a placeholder converter - it will be replaced in the sink
        OpenGeminiPointConverter<RowData> placeholderConverter =
                new OpenGeminiPointConverter<RowData>() {
                    @Override
                    public Point convert(RowData element, String measurement) {
                        return null;
                    }
                };

        OpenGeminiSinkConfiguration<RowData> sinkConfig =
                configBuilder
                        .converter(placeholderConverter) // Use placeholder instead of null
                        .build();

        // Extract field mapping
        FieldMappingConfig fieldMapping = extractFieldMapping(config, context);
        Integer parallelism = config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null);

        DynamicTableSink sink =
                new OpenGeminiDynamicTableSink(
                        sinkConfig,
                        context.getCatalogTable().getResolvedSchema(),
                        fieldMapping,
                        parallelism);

        return sink;
    }

    /** Extract field mapping configuration from options */
    private FieldMappingConfig extractFieldMapping(ReadableConfig config, Context context) {
        FieldMappingConfig.Builder builder = FieldMappingConfig.builder();

        // Set timestamp field
        config.getOptional(TIMESTAMP_FIELD).ifPresent(builder::timestampField);

        // Parse tag fields
        config.getOptional(TAG_FIELDS)
                .ifPresent(
                        tagFieldsStr -> {
                            String[] tags = tagFieldsStr.split(TAG_FIELD_DELIMITER);
                            for (String tag : tags) {
                                builder.addTagField(tag.trim());
                            }
                        });

        // Parse field fields
        config.getOptional(FIELD_FIELDS)
                .ifPresent(
                        fieldFieldsStr -> {
                            String[] fields = fieldFieldsStr.split(TAG_FIELD_DELIMITER);
                            for (String field : fields) {
                                builder.addFieldField(field.trim());
                            }
                        });

        // Set other options
        builder.ignoreNullValues(config.get(IGNORE_NULL_VALUES));
        builder.writePrecision(config.get(WRITE_PRECISION));

        return builder.build();
    }

    /** Helper class to store field mapping configuration */
    public static class FieldMappingConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String timestampField;
        private final Set<String> tagFields;
        private final Set<String> fieldFields;
        private final boolean ignoreNullValues;
        private final String writePrecision;

        private FieldMappingConfig(Builder builder) {
            this.timestampField = builder.timestampField;
            this.tagFields = builder.tagFields;
            this.fieldFields = builder.fieldFields;
            this.ignoreNullValues = builder.ignoreNullValues;
            this.writePrecision = builder.writePrecision;
        }

        // Getters
        public String getTimestampField() {
            return timestampField;
        }

        public Set<String> getTagFields() {
            return tagFields;
        }

        public Set<String> getFieldFields() {
            return fieldFields;
        }

        public boolean isIgnoreNullValues() {
            return ignoreNullValues;
        }

        public String getWritePrecision() {
            return writePrecision;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String timestampField;
            private Set<String> tagFields = new HashSet<>();
            private Set<String> fieldFields = new HashSet<>();
            private boolean ignoreNullValues = true;
            private String writePrecision = "ms";

            public Builder timestampField(String field) {
                this.timestampField = field;
                return this;
            }

            public Builder addTagField(String field) {
                this.tagFields.add(field);
                return this;
            }

            public Builder addFieldField(String field) {
                this.fieldFields.add(field);
                return this;
            }

            public Builder ignoreNullValues(boolean ignore) {
                this.ignoreNullValues = ignore;
                return this;
            }

            public Builder writePrecision(String precision) {
                this.writePrecision = precision;
                return this;
            }

            public FieldMappingConfig build() {
                return new FieldMappingConfig(this);
            }
        }
    }
}
