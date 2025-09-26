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

import static org.opengemini.flink.sink.SimpleOpenGeminiLineProtocolConverter.escape;
import static org.opengemini.flink.sink.SimpleOpenGeminiLineProtocolConverter.formatField;

import java.io.Serializable;
import java.util.*;

import javax.annotation.Nullable;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.opengemini.flink.sink.OpenGeminiLineProtocolConverter;
import org.opengemini.flink.sink.OpenGeminiPointConverter;
import org.opengemini.flink.sink.OpenGeminiSink;
import org.opengemini.flink.sink.OpenGeminiSinkConfiguration;
import org.opengemini.flink.table.OpenGeminiDynamicTableSinkFactory.FieldMappingConfig;

import io.opengemini.client.api.Point;

/** Dynamic table sink for OpenGemini that bridges Table API with DataStream API. */
public class OpenGeminiDynamicTableSink implements DynamicTableSink, Serializable {
    private static final long serialVersionUID = 1L;
    private final OpenGeminiSinkConfiguration<RowData> sinkConfiguration;
    private final ResolvedSchema tableSchema;
    private final FieldMappingConfig fieldMapping;
    private String converterType;
    @Nullable private final Integer parallelism;

    public static final String DEFAULT_CONVERTER_TYPE = "line-protocol";

    public OpenGeminiDynamicTableSink(
            OpenGeminiSinkConfiguration<RowData> sinkConfiguration,
            ResolvedSchema tableSchema,
            FieldMappingConfig fieldMapping,
            @Nullable Integer parallelism,
            String converterType) {
        this.sinkConfiguration = Preconditions.checkNotNull(sinkConfiguration);
        this.tableSchema = Preconditions.checkNotNull(tableSchema);
        this.fieldMapping = Preconditions.checkNotNull(fieldMapping);
        this.parallelism = parallelism;
        this.converterType = converterType != null ? converterType : DEFAULT_CONVERTER_TYPE;
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
        OpenGeminiSink<RowData> sink;

        if ("line-protocol".equals(converterType)) {
            RowDataToLineProtocolConverter converter =
                    new RowDataToLineProtocolConverter(
                            tableSchema, fieldMapping, sinkConfiguration.getMeasurement());
            sink = new OpenGeminiSink<>(sinkConfiguration, converter);
        } else {
            RowDataToPointConverter converter =
                    new RowDataToPointConverter(
                            tableSchema, fieldMapping, sinkConfiguration.getMeasurement());
            sink = new OpenGeminiSink<>(sinkConfiguration, converter);
        }

        return SinkFunctionProvider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new OpenGeminiDynamicTableSink(
                sinkConfiguration, tableSchema, fieldMapping, parallelism, converterType);
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
    public static class RowDataToPointConverter extends AbstractRowDataConverter
            implements OpenGeminiPointConverter<RowData>, Serializable {

        public RowDataToPointConverter(
                ResolvedSchema schema, FieldMappingConfig fieldMapping, String measurement) {
            super(schema, fieldMapping, measurement);
        }

        @Override
        public Point convertToPoint(RowData rowData, String measurementName) {
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
            Map<String, java.lang.Object> fields = new HashMap<>();
            for (String fieldColumn : fieldColumns) {
                Integer index = columnIndexMap.get(fieldColumn);
                if (index != null) {
                    if (fieldMapping.isIgnoreNullValues() && rowData.isNullAt(index)) {
                        continue;
                    }

                    java.lang.Object value =
                            extractFieldValue(rowData, index, columnTypes.get(index));
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
    }

    public static class RowDataToLineProtocolConverter extends AbstractRowDataConverter
            implements OpenGeminiLineProtocolConverter<RowData>, Serializable {

        public RowDataToLineProtocolConverter(
                ResolvedSchema schema, FieldMappingConfig fieldMapping, String measurement) {
            super(schema, fieldMapping, measurement);
        }

        @Override
        public String convertToLineProtocol(RowData rowData, String measurementName) {
            if (rowData.getRowKind() == RowKind.DELETE
                    || rowData.getRowKind() == RowKind.UPDATE_BEFORE) {
                return null;
            }

            StringBuilder sb = new StringBuilder();

            // Measurement
            sb.append(measurement);

            // Tags - directly append
            for (String tagColumn : tagColumns) {
                Integer index = columnIndexMap.get(tagColumn);
                if (index != null && !rowData.isNullAt(index)) {
                    String tagValue = extractStringValue(rowData, index, columnTypes.get(index));
                    if (tagValue != null) {
                        sb.append(',').append(tagColumn).append('=').append(escape(tagValue));
                    }
                }
            }

            // Fields - direct append
            sb.append(' ');
            StringJoiner fieldJoiner = new StringJoiner(",");
            for (String fieldColumn : fieldColumns) {
                Integer index = columnIndexMap.get(fieldColumn);
                if (index != null && !rowData.isNullAt(index)) {
                    Object value = extractFieldValue(rowData, index, columnTypes.get(index));
                    if (value != null) {
                        fieldJoiner.add(formatField(fieldColumn, value));
                    }
                }
            }

            if (fieldJoiner.length() == 0) {
                return null; // At least one field required
            }
            sb.append(fieldJoiner.toString());

            // Timestamp
            sb.append(' ').append(extractTimestamp(rowData));

            return sb.toString();
        }
    }
}
