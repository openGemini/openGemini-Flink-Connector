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

import java.lang.reflect.Method;
import java.util.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenGeminiDynamicTableSinkFactoryTest {

    private OpenGeminiDynamicTableSinkFactory factory;

    @BeforeEach
    void setUp() {
        factory = new OpenGeminiDynamicTableSinkFactory();
    }

    @Test
    void testFactoryIdentifierAndOptionsExposure() {
        // identifier
        assertEquals(OpenGeminiDynamicTableSinkFactory.IDENTIFIER, factory.factoryIdentifier());

        // required options contain host, database, measurement
        Set<?> required = factory.requiredOptions();
        assertTrue(required.contains(OpenGeminiDynamicTableSinkFactory.HOST));
        assertTrue(required.contains(OpenGeminiDynamicTableSinkFactory.DATABASE));
        assertTrue(required.contains(OpenGeminiDynamicTableSinkFactory.MEASUREMENT));

        // optional options contain some expected keys
        Set<?> optional = factory.optionalOptions();
        assertTrue(optional.contains(OpenGeminiDynamicTableSinkFactory.PORT));
        assertTrue(optional.contains(OpenGeminiDynamicTableSinkFactory.USERNAME));
        assertTrue(optional.contains(OpenGeminiDynamicTableSinkFactory.PASSWORD));
        assertTrue(optional.contains(OpenGeminiDynamicTableSinkFactory.BATCH_SIZE));
    }

    @Test
    void testExtractFieldMapping_parsesTagAndFieldFieldsAndOptions() throws Exception {
        // Build a Flink Configuration (implements ReadableConfig)
        Configuration cfg = new Configuration();
        cfg.set(OpenGeminiDynamicTableSinkFactory.TAG_FIELDS, "tag1, tag2 ,tag3");
        cfg.set(OpenGeminiDynamicTableSinkFactory.FIELD_FIELDS, "f1,f2");
        cfg.set(OpenGeminiDynamicTableSinkFactory.TIMESTAMP_FIELD, "event_time");
        cfg.set(OpenGeminiDynamicTableSinkFactory.IGNORE_NULL_VALUES, false);
        cfg.set(OpenGeminiDynamicTableSinkFactory.WRITE_PRECISION, "us");

        // extractFieldMapping is private; use reflection to invoke it
        Method m =
                OpenGeminiDynamicTableSinkFactory.class.getDeclaredMethod(
                        "extractFieldMapping",
                        ReadableConfig.class,
                        DynamicTableSinkFactory.Context.class);
        m.setAccessible(true);

        // context is not used by the method, pass a mock
        DynamicTableSinkFactory.Context mockContext = mock(DynamicTableSinkFactory.Context.class);

        Object result = m.invoke(factory, cfg, mockContext);
        assertNotNull(result);
        assertTrue(result instanceof OpenGeminiDynamicTableSinkFactory.FieldMappingConfig);

        OpenGeminiDynamicTableSinkFactory.FieldMappingConfig fm =
                (OpenGeminiDynamicTableSinkFactory.FieldMappingConfig) result;

        assertEquals("event_time", fm.getTimestampField());
        assertEquals("us", fm.getWritePrecision());
        assertFalse(fm.isIgnoreNullValues());

        // tags and fields sets should contain trimmed values
        assertTrue(fm.getTagFields().contains("tag1"));
        assertTrue(fm.getTagFields().contains("tag2"));
        assertTrue(fm.getTagFields().contains("tag3"));
        assertTrue(fm.getFieldFields().contains("f1"));
        assertTrue(fm.getFieldFields().contains("f2"));
    }

    @Test
    void testCreateDynamicTableSink_withPartialAuthentication() {
        // First, let's add debug logging to see what's missing
        Map<String, String> options1 = new HashMap<>();
        options1.put("connector", "opengemini");
        options1.put("host", "localhost");
        options1.put("database", "testdb");
        options1.put("measurement", "test_measurement");
        options1.put("username", "user"); // Only username, no password

        DynamicTableSinkFactory.Context context1 = createCompleteMockContext(options1);

        // Add try-catch to see exact issue
        try {
            DynamicTableSink sink1 = factory.createDynamicTableSink(context1);
            assertNotNull(sink1);
        } catch (NullPointerException e) {
            e.printStackTrace();
            fail("NPE occurred: " + e.getMessage());
        }
    }

    // Complete mock context that covers all FactoryUtil requirements
    private DynamicTableSinkFactory.Context createCompleteMockContext(Map<String, String> options) {
        DynamicTableSinkFactory.Context context = mock(DynamicTableSinkFactory.Context.class);

        // 1. Mock ObjectIdentifier (CRITICAL - often missing)
        ObjectIdentifier identifier =
                ObjectIdentifier.of("default_catalog", "default_database", "test_table");
        when(context.getObjectIdentifier()).thenReturn(identifier);

        // 2. Mock Configuration
        Configuration configuration = Configuration.fromMap(options);
        when(context.getConfiguration()).thenReturn(configuration);

        // 3. Mock CatalogTable with ResolvedSchema
        ResolvedSchema resolvedSchema = createMockResolvedSchema();
        ResolvedCatalogTable catalogTable = createMockCatalogTable(resolvedSchema, options);
        when(context.getCatalogTable()).thenReturn(catalogTable);

        // 4. Mock ClassLoader (sometimes needed)
        when(context.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());

        // 5. Mock isTemporary
        when(context.isTemporary()).thenReturn(false);

        return context;
    }

    private ResolvedSchema createMockResolvedSchema() {
        // Create a proper schema with columns
        Column col1 = Column.physical("id", DataTypes.INT());
        Column col2 = Column.physical("name", DataTypes.STRING());
        Column col3 = Column.physical("timestamp", DataTypes.TIMESTAMP(3));

        List<Column> columns = Arrays.asList(col1, col2, col3);

        // Use ResolvedSchema.of() if available, otherwise mock
        try {
            return ResolvedSchema.of(columns);
        } catch (Exception e) {
            // If static method not available, mock it
            ResolvedSchema schema = mock(ResolvedSchema.class);
            when(schema.getColumns()).thenReturn(columns);
            return schema;
        }
    }

    private ResolvedCatalogTable createMockCatalogTable(
            ResolvedSchema schema, Map<String, String> options) {
        ResolvedCatalogTable catalogTable = mock(ResolvedCatalogTable.class);

        // Return the resolved schema
        when(catalogTable.getResolvedSchema()).thenReturn(schema);

        // Return options
        when(catalogTable.getOptions()).thenReturn(options);

        // Return partition keys (usually empty for sink)
        when(catalogTable.getPartitionKeys()).thenReturn(Collections.emptyList());

        return catalogTable;
    }

    @Test
    void testFieldMappingConfigBuilder() {
        // Test the builder pattern
        OpenGeminiDynamicTableSinkFactory.FieldMappingConfig config =
                OpenGeminiDynamicTableSinkFactory.FieldMappingConfig.builder()
                        .timestampField("ts")
                        .addTagField("tag1")
                        .addTagField("tag2")
                        .addFieldField("field1")
                        .ignoreNullValues(false)
                        .writePrecision("us")
                        .build();

        assertEquals("ts", config.getTimestampField());
        assertEquals(2, config.getTagFields().size());
        assertTrue(config.getTagFields().contains("tag1"));
        assertFalse(config.isIgnoreNullValues());
        assertEquals("us", config.getWritePrecision());
    }

    @Test
    void testFieldMappingConfigBuilder_withDefaults() {
        // Test default values
        OpenGeminiDynamicTableSinkFactory.FieldMappingConfig config =
                OpenGeminiDynamicTableSinkFactory.FieldMappingConfig.builder().build();

        assertNull(config.getTimestampField());
        assertTrue(config.getTagFields().isEmpty());
        assertTrue(config.getFieldFields().isEmpty());
        assertTrue(config.isIgnoreNullValues()); // Default is true
        assertEquals("ms", config.getWritePrecision()); // Default is "ms"
    }

    @Test
    void testCreateDynamicTableSink_withMinimalConfig() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "opengemini");
        options.put("host", "localhost");
        options.put("database", "testdb");
        options.put("measurement", "test_measurement");

        DynamicTableSinkFactory.Context context = createCompleteMockContext(options);

        DynamicTableSink sink = factory.createDynamicTableSink(context);

        assertNotNull(sink);
        assertTrue(sink instanceof OpenGeminiDynamicTableSink);
    }

    @Test
    void testCreateDynamicTableSink_withFullConfig() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "opengemini");
        options.put("host", "localhost");
        options.put("port", "8087");
        options.put("database", "testdb");
        options.put("measurement", "test_measurement");
        options.put("username", "user");
        options.put("password", "pass");
        options.put("batch-size", "500");
        options.put("flush-interval", "2s");
        options.put("max-retries", "5");
        options.put("timestamp-field", "ts");
        options.put("tag-fields", "tag1,tag2");
        options.put("field-fields", "field1,field2");
        options.put("ignore-null-values", "false");
        options.put("write-precision", "ns");

        DynamicTableSinkFactory.Context context = createCompleteMockContext(options);

        DynamicTableSink sink = factory.createDynamicTableSink(context);

        assertNotNull(sink);
        assertTrue(sink instanceof OpenGeminiDynamicTableSink);
    }

    @Test
    void testExtractFieldMapping_withEmptyFields() throws Exception {
        Configuration cfg = new Configuration();

        Method m =
                OpenGeminiDynamicTableSinkFactory.class.getDeclaredMethod(
                        "extractFieldMapping",
                        ReadableConfig.class,
                        DynamicTableSinkFactory.Context.class);
        m.setAccessible(true);

        DynamicTableSinkFactory.Context mockContext = mock(DynamicTableSinkFactory.Context.class);

        Object result = m.invoke(factory, cfg, mockContext);
        OpenGeminiDynamicTableSinkFactory.FieldMappingConfig fm =
                (OpenGeminiDynamicTableSinkFactory.FieldMappingConfig) result;

        assertNull(fm.getTimestampField());
        assertTrue(fm.getTagFields().isEmpty());
        assertTrue(fm.getFieldFields().isEmpty());
        assertTrue(fm.isIgnoreNullValues()); // Default value
        assertEquals("ms", fm.getWritePrecision()); // Default value
    }
}
