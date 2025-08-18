# Flink OpenGemini Connector (DRAFT)

_This connector is still being developed. Functions described may not be fully implemented yet, or
subject to change. Please check the latest documentation and source code for updates._

A high-performance Apache Flink sink connector for [OpenGemini](https://github.com/openGemini/openGemini) time-series database.

## Features

- **High Performance**: Optimized batch writing for efficient data ingestion
- **Fault Tolerance**: Integrated with Flink's checkpoint mechanism for at-least-once delivery
- **Flexible Configuration**: Multiple configuration methods including properties files, command line, and programmatic
- **Type Conversion**: Pluggable converter interface for custom data transformations
- **Async Writing**: Non-blocking writes with configurable parallelism
- **Error Handling**: Automatic retry for transient failures with exponential backoff
- **Monitoring and Metrics**: Exposes metrics for monitoring write performance and errors

## Quick Start

### Maven Dependency

To be supported

### Basic Usage

#### Programmatic Configuration

```java
import org.opengemini.flink.sink.*;

// Create sink configuration using builder
OpenGeminiSinkConfiguration<MyData> config = OpenGeminiSinkConfiguration.<MyData>builder()
    .setHost("localhost")
    .setPort(8086)
    .setDatabase("mydb")
    .setMeasurement("mymeasurement")
    .setBatchSize(5000)
    .setFlushInterval(100, TimeUnit.MILLISECONDS)
    .setConverter(new MyDataConverter())
    .build();

// Add sink to your Flink job
DataStream<MyData> stream = ...;
stream.addSink(new OpenGeminiSink<>(config))
    .name("OpenGemini Sink");
```

#### Using Properties File

Create a `opengemini-connector.properties` file:

```properties
# OpenGemini Connection Settings
opengemini.connector.host=localhost
opengemini.connector.port=8086
opengemini.connector.database=mydb
opengemini.connector.measurement=mymeasurement

# Optional Authentication
opengemini.connector.username=admin
opengemini.connector.password=secret

# Performance Tuning
opengemini.connector.batch.size=10000
opengemini.connector.flush.interval.ms=200
opengemini.connector.max.retries=5

# Timeouts
opengemini.connector.connection.timeout.ms=10000
opengemini.connector.request.timeout.ms=60000
```

Load configuration from file:

```java
// Load from default locations (current directory or classpath)
OpenGeminiSinkConfiguration<MyData> config =
    OpenGeminiSinkConfiguration.createDefaultConfiguration(new MyDataConverter());

// Or load from specific file
OpenGeminiSinkConfiguration<MyData> config =
    OpenGeminiSinkConfiguration.fromPropertiesFile("config/my-config.properties", new MyDataConverter());
```

#### Using Command Line Arguments

```java
// Using Flink's ParameterTool
ParameterTool params = ParameterTool.fromArgs(args);
OpenGeminiSinkConfiguration<MyData> config =
    OpenGeminiSinkConfiguration.fromParameterTool(params, new MyDataConverter());
```

Run with command line arguments:
```bash
flink run myapp.jar \
  --opengemini.connector.database mydb \
  --opengemini.connector.measurement mymeasurement \
  --opengemini.connector.host prod-server \
  --opengemini.connector.batch.size 20000
```

#### Mixed Configuration Sources

```java
// Load base configuration from file, override with command line
Properties props = new Properties();
props.load(new FileInputStream("base-config.properties"));

ParameterTool params = ParameterTool.fromArgs(args);

OpenGeminiSinkConfiguration<MyData> config =
    OpenGeminiSinkConfiguration.fromMixedSources(params, props, new MyDataConverter());
```

### Implementing a Converter

```java
public class MyDataConverter implements OpenGeminiPointConverter<MyData> {
    @Override
    public Point convert(MyData data, String measurement) {
        Point point = new Point();
        point.setMeasurement(measurement);
        point.setTime(data.getTimestamp());

        // Add tags
        Map<String, String> tags = new HashMap<>();
        tags.put("sensor", data.getSensorId());
        point.setTags(tags);

        // Add fields
        Map<String, Object> fields = new HashMap<>();
        fields.put("temperature", data.getTemperature());
        fields.put("humidity", data.getHumidity());
        point.setFields(fields);

        return point;
    }
}
```

## Configuration Options

| Property | Default | Description |
|----------|---------|-------------|
| `opengemini.connector.host` | localhost | OpenGemini server host |
| `opengemini.connector.port` | 8086 | OpenGemini server port |
| `opengemini.connector.database` | - | Target database name (required) |
| `opengemini.connector.measurement` | - | Target measurement name (required) |
| `opengemini.connector.username` | - | Username for authentication |
| `opengemini.connector.password` | - | Password for authentication |
| `opengemini.connector.batch.size` | 5000 | Number of points to batch before writing |
| `opengemini.connector.flush.interval.ms` | 100 | Maximum time to wait before flushing (milliseconds) |
| `opengemini.connector.max.retries` | 3 | Maximum retry attempts for failed writes |
| `opengemini.connector.connection.timeout.ms` | 5000 | HTTP connection timeout (milliseconds) |
| `opengemini.connector.request.timeout.ms` | 30000 | HTTP request timeout (milliseconds) |

## Configuration File Locations

When using `createDefaultConfiguration()`, the connector searches for `opengemini-connector.properties` in:
1. Current working directory
2. Classpath (typically `src/main/resources`)

根据代码，需要添加以下 Table API/SQL 支持部分：

## Table API / SQL Support

### Creating Table with DDL

```sql
CREATE TABLE sensor_data (
    sensor_id STRING,
    location STRING,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'opengemini',
    'host' = 'localhost',
    'port' = '8086',
    'database' = 'mydb',
    'measurement' = 'sensors',
    'username' = 'admin',
    'password' = 'secret',

    -- Field mapping
    'timestamp-field' = 'ts',
    'tag-fields' = 'sensor_id,location',
    'field-fields' = 'temperature,humidity,pressure',

    -- Write options
    'batch-size' = '5000',
    'flush-interval' = '1s',
    'ignore-null-values' = 'true',
    'write-precision' = 'ms',

    -- Performance
    'max-retries' = '3',
    'connection-timeout' = '10s',
    'request-timeout' = '30s'
);

-- Insert data
INSERT INTO sensor_data
SELECT sensor_id, location, temp, humidity, pressure, CURRENT_TIMESTAMP
FROM source_table;
```

### Table API Usage

```java
import org.apache.flink.table.api.*;

TableEnvironment tableEnv = TableEnvironment.create(settings);

// Define table with connector
tableEnv.executeSql(
    "CREATE TABLE measurements (" +
    "  device_id STRING," +
    "  metric_name STRING," +
    "  value DOUBLE," +
    "  event_time TIMESTAMP(3)" +
    ") WITH (" +
    "  'connector' = 'opengemini'," +
    "  'host' = 'localhost'," +
    "  'database' = 'metrics'," +
    "  'measurement' = 'device_metrics'," +
    "  'tag-fields' = 'device_id,metric_name'," +
    "  'timestamp-field' = 'event_time'" +
    ")"
);

// Write data using Table API
Table sourceTable = tableEnv.from("source");
sourceTable.insertInto("measurements").execute();
```

### Field Mapping Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `timestamp-field` | - | Column to use as timestamp (uses processing time if not set) |
| `tag-fields` | - | Comma-separated list of columns to use as OpenGemini tags |
| `field-fields` | - | Comma-separated list of columns to use as OpenGemini fields (default: all non-tag columns) |
| `write-precision` | `ms` | Timestamp precision: `ns`, `us`, `ms`, `s`, `m`, `h` |
| `ignore-null-values` | `true` | Whether to skip null values when writing |

### Supported Data Types

| Flink SQL Type | OpenGemini Field Type | Notes |
|----------------|----------------------|-------|
| BOOLEAN | Boolean | |
| TINYINT, SMALLINT, INTEGER | Integer | |
| BIGINT | Long | |
| FLOAT | Float | |
| DOUBLE | Double | |
| DECIMAL | Decimal | |
| VARCHAR, CHAR | String | |
| TIMESTAMP | Timestamp | Converted based on `write-precision` |

### Changelog Support

The connector supports the following row kinds:
- **INSERT**: Written as new points
- **UPDATE_AFTER**: Written as new points (upsert behavior)
- **UPDATE_BEFORE**: Ignored
- **DELETE**: Ignored


## Building from Source

```bash
git clone https://github.com/apache/flink-connector-opengemini.git
cd flink-connector-opengemini
mvn clean install
```

### Running Tests

```bash
mvn test
```

## Checkpointing

The connector integrates with Flink's checkpoint mechanism:

```java
// Enable checkpointing in your Flink job
env.enableCheckpointing(60000); // 60 seconds
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
```

During checkpoints, the sink will flush all buffered data to ensure no data loss in case of failures.

## Monitoring

The connector exposes the following Flink metrics under `opengemini.sink`:

| Metric | Type | Description |
|--------|------|-------------|
| `writeLatency` | Histogram | Batch write latency in milliseconds |
| `currentBatchSize` | Gauge | Current number of points in buffer |
| `writeErrors` | Counter | Total number of write failures |
| `lastSuccessfulWriteTime` | Gauge | Timestamp of last successful write |
| `pointsPerSecond` | Meter | Write throughput (60-second window) |
| `totalBytesWritten` | Gauge | Total bytes written to OpenGemini |

Access metrics via:
- Flink Web UI: Navigate to Task Metrics
- REST API: `/jobs/:jobid/metrics`
- Export to monitoring systems (Prometheus, Graphite, etc.)

## Known Limitations

- Currently supports at-least-once delivery semantics only
- No support for schema evolution

## Roadmap

- [ ] Load balancing across multiple OpenGemini nodes
- [ ] Adaptive batching based on load

## Requirements

- Apache Flink 1.18+
- Java 8+
- OpenGemini
