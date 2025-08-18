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
import java.util.*;

import javax.annotation.Nullable;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.opengemini.flink.sink.OpenGeminiPointConverter;
import org.opengemini.flink.sink.OpenGeminiSink;
import org.opengemini.flink.sink.OpenGeminiSinkConfiguration;
import org.opengemini.flink.table.OpenGeminiDynamicTableSinkFactory.FieldMappingConfig;

import io.opengemini.client.api.Point;
import io.opengemini.client.api.Precision;

/** Dynamic table sink for OpenGemini that bridges Table API with DataStream API. */
public class OpenGeminiDynamicTableSink implements DynamicTableSink, Serializable {
    private static final long serialVersionUID = 1L;
    private final OpenGeminiSinkConfiguration<RowData> sinkConfiguration;
    private final ResolvedSchema tableSchema;
    private final FieldMappingConfig fieldMapping;
    @Nullable private final Integer parallelism;

    public static final String PRECISION_NANOSECOND = "ns";
    public static final String PRECISION_MICROSECOND = "us";
    public static final String PRECISION_MILLISECOND = "ms";
    public static final String PRECISION_SECOND = "s";
    public static final String PRECISION_MINUTE = "m";
    public static final String PRECISION_HOUR = "h";

    public OpenGeminiDynamicTableSink(
            OpenGeminiSinkConfiguration<RowData> sinkConfiguration,
            ResolvedSchema tableSchema,
            FieldMappingConfig fieldMapping,
            @Nullable Integer parallelism) {
        this.sinkConfiguration = Preconditions.checkNotNull(sinkConfiguration);
        this.tableSchema = Preconditions.checkNotNull(tableSchema);
        this.fieldMapping = Preconditions.checkNotNull(fieldMapping);
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // OpenGemini primarily handles inserts and updates
        // We can handle INSERT and UPDATE_AFTER, ignore UPDATE_BEFORE and DELETE
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        builder.addContainedKind(RowKind.INSERT);
        builder.addContainedKind(RowKind.UPDATE_AFTER);

        // Optionally support UPSERT mode if all rows are treated as inserts/updates
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // Create RowData to Point converter
        RowDataToPointConverter converter =
                new RowDataToPointConverter(
                        tableSchema, fieldMapping, sinkConfiguration.getMeasurement());

        // Create a new configuration with the converter
        OpenGeminiSinkConfiguration<RowData> configWithConverter =
                sinkConfiguration.toBuilder().converter(converter).build();

        // Create the actual sink function
        OpenGeminiSink<RowData> sink = new OpenGeminiSink<>(configWithConverter, converter);

        // Return as SinkFunctionProvider with optional parallelism
        return SinkFunctionProvider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new OpenGeminiDynamicTableSink(
                sinkConfiguration, tableSchema, fieldMapping, parallelism);
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "OpenGemini Table Sink(host=%s:%d, database=%s, measurement=%s)",
                sinkConfiguration.getHost(),
                sinkConfiguration.getPort(),
                sinkConfiguration.getDatabase(),
                sinkConfiguration.getMeasurement());
    }

    /** Converter that transforms RowData to OpenGemini Point */
    public static class RowDataToPointConverter
            implements OpenGeminiPointConverter<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final FieldMappingConfig fieldMapping;
        private final String measurement;
        private final List<String> columnNames;
        private final List<LogicalType> columnTypes;
        private final Set<String> tagColumns;
        private final Set<String> fieldColumns;
        private final int timestampFieldIndex;
        private final Map<String, Integer> columnIndexMap;

        public RowDataToPointConverter(
                ResolvedSchema schema, FieldMappingConfig fieldMapping, String measurement) {
            this.fieldMapping = fieldMapping;
            this.measurement = measurement;

            // Extract column information
            this.columnNames = schema.getColumnNames();
            this.columnTypes =
                    schema.getColumnDataTypes().stream()
                            .map(DataType::getLogicalType)
                            .collect(java.util.stream.Collectors.toList());

            // Build column index map for quick lookup
            this.columnIndexMap = new HashMap<>();
            for (int i = 0; i < columnNames.size(); i++) {
                columnIndexMap.put(columnNames.get(i), i);
            }

            // Determine tag and field columns
            this.tagColumns = new HashSet<>(fieldMapping.getTagFields());
            this.fieldColumns = new HashSet<>();

            if (!fieldMapping.getFieldFields().isEmpty()) {
                this.fieldColumns.addAll(fieldMapping.getFieldFields());
            } else {
                // If not specified, all non-tag columns are field columns
                for (String col : columnNames) {
                    if (!tagColumns.contains(col)
                            && !col.equals(fieldMapping.getTimestampField())) {
                        fieldColumns.add(col);
                    }
                }
            }

            // Find timestamp field index
            String timestampField = fieldMapping.getTimestampField();
            if (timestampField != null && columnIndexMap.containsKey(timestampField)) {
                this.timestampFieldIndex = columnIndexMap.get(timestampField);
            } else {
                this.timestampFieldIndex = -1; // Will use current time
            }
        }

        @Override
        public Point convert(RowData rowData, String measurementName) {
            // Skip deleted rows
            if (rowData.getRowKind() == RowKind.DELETE
                    || rowData.getRowKind() == RowKind.UPDATE_BEFORE) {
                return null;
            }

            Point point = new Point();
            point.setMeasurement(measurement);

            // Set timestamp
            long timestamp = extractTimestamp(rowData);
            point.setTime(timestamp);
            point.setPrecision(getPrecision());

            // Prepare tags map
            Map<String, String> tags = new HashMap<>();
            for (String tagColumn : tagColumns) {
                Integer index = columnIndexMap.get(tagColumn);
                // if null tag, skip it
                if (index != null && !rowData.isNullAt(index)) {
                    String tagValue = extractStringValue(rowData, index, columnTypes.get(index));
                    if (tagValue != null) {
                        tags.put(tagColumn, tagValue);
                    }
                }
            }
            if (!tags.isEmpty()) {
                point.setTags(tags);
            }

            // Prepare fields map
            Map<String, Object> fields = new HashMap<>();
            for (String fieldColumn : fieldColumns) {
                Integer index = columnIndexMap.get(fieldColumn);
                if (index != null) {
                    if (fieldMapping.isIgnoreNullValues() && rowData.isNullAt(index)) {
                        continue;
                    }

                    Object value = extractFieldValue(rowData, index, columnTypes.get(index));
                    if (value != null) {
                        fields.put(fieldColumn, value);
                    }
                }
            }

            // OpenGemini requires at least one field
            if (fields.isEmpty()) {
                fields.put("_empty", true);
            }
            point.setFields(fields);

            return point;
        }

        private Precision getPrecision() {
            switch (fieldMapping.getWritePrecision()) {
                case PRECISION_NANOSECOND:
                    return Precision.PRECISIONNANOSECOND;
                case PRECISION_MICROSECOND:
                    return Precision.PRECISIONMICROSECOND;
                case PRECISION_MILLISECOND:
                    return Precision.PRECISIONMILLISECOND;
                case PRECISION_SECOND:
                    return Precision.PRECISIONSECOND;
                case PRECISION_MINUTE:
                    return Precision.PRECISIONMINUTE;
                case PRECISION_HOUR:
                    return Precision.PRECISIONHOUR;
                default:
                    return Precision.PRECISIONMILLISECOND;
            }
        }

        private long extractTimestamp(RowData rowData) {
            if (timestampFieldIndex >= 0 && !rowData.isNullAt(timestampFieldIndex)) {
                LogicalType type = columnTypes.get(timestampFieldIndex);
                switch (type.getTypeRoot()) {
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        // Flink timestamps are in milliseconds
                        return rowData.getTimestamp(timestampFieldIndex, 3).getMillisecond();
                    case TIMESTAMP_WITH_TIME_ZONE:
                        return rowData.getTimestamp(timestampFieldIndex, 3).getMillisecond();
                    case BIGINT:
                        // Assume bigint timestamps are in the configured precision
                        long ts = rowData.getLong(timestampFieldIndex);
                        return convertToNanos(ts);
                    default:
                        // Fallback to current time
                        return System.currentTimeMillis();
                }
            }
            // Use current time if no timestamp field
            return System.currentTimeMillis();
        }

        private String extractStringValue(RowData rowData, int index, LogicalType type) {
            if (rowData.isNullAt(index)) {
                return null;
            }

            switch (type.getTypeRoot()) {
                case VARCHAR:
                case CHAR:
                    return rowData.getString(index).toString();
                case BOOLEAN:
                    return String.valueOf(rowData.getBoolean(index));
                case TINYINT:
                    return String.valueOf(rowData.getByte(index));
                case SMALLINT:
                    return String.valueOf(rowData.getShort(index));
                case INTEGER:
                    return String.valueOf(rowData.getInt(index));
                case BIGINT:
                    return String.valueOf(rowData.getLong(index));
                case FLOAT:
                    return String.valueOf(rowData.getFloat(index));
                case DOUBLE:
                    return String.valueOf(rowData.getDouble(index));
                default:
                    return rowData.getString(index).toString();
            }
        }

        private Object extractFieldValue(RowData rowData, int index, LogicalType type) {
            if (rowData.isNullAt(index)) {
                return null;
            }

            switch (type.getTypeRoot()) {
                case BOOLEAN:
                    return rowData.getBoolean(index);
                case TINYINT:
                    return (int) rowData.getByte(index);
                case SMALLINT:
                    return (int) rowData.getShort(index);
                case INTEGER:
                    return rowData.getInt(index);
                case BIGINT:
                    return rowData.getLong(index);
                case FLOAT:
                    return rowData.getFloat(index);
                case DOUBLE:
                    return rowData.getDouble(index);
                case VARCHAR:
                case CHAR:
                    return rowData.getString(index).toString();
                case DECIMAL:
                    return rowData.getDecimal(
                            index,
                            ((org.apache.flink.table.types.logical.DecimalType) type)
                                    .getPrecision(),
                            ((org.apache.flink.table.types.logical.DecimalType) type).getScale());
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                    // Convert timestamp to string in ISO format
                    return rowData.getTimestamp(index, 3).toLocalDateTime().toString();
                default:
                    // Fallback to string representation
                    return rowData.getString(index).toString();
            }
        }

        private long convertToNanos(long timestamp) {
            // Convert based on configured precision to nanoseconds
            switch (fieldMapping.getWritePrecision()) {
                case PRECISION_NANOSECOND:
                    return timestamp;
                case PRECISION_MICROSECOND:
                    return timestamp * 1000;
                case PRECISION_MILLISECOND:
                    return timestamp * 1_000_000;
                case PRECISION_SECOND:
                    return timestamp * 1_000_000_000;
                default:
                    return timestamp * 1_000_000; // Default ms
            }
        }
    }
}
