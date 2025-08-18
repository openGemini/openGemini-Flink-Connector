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

The connector exposes the following metrics (when integrated):

- `recordsWritten`: Total number of records successfully written
- `writeErrors`: Total number of write failures
- `batchSize`: Histogram of batch sizes
- `writeLatency`: Histogram of write latencies

## Known Limitations

- Currently supports at-least-once delivery semantics only
- Table API/SQL support is not yet implemented
- No support for schema evolution

## Roadmap

- [ ] Table API and SQL support
- [ ] Exactly-once semantics with two-phase commit
- [ ] Load balancing across multiple OpenGemini nodes
- [ ] Adaptive batching based on load
- [ ] Flink metrics integration

## Requirements

- Apache Flink 1.17+
- Java 8+
- OpenGemini