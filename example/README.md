# OpenGemini Flink Connector Examples

This module contains practical examples demonstrating how to use the OpenGemini Flink Sink Connector to stream data from Apache Flink to OpenGemini time-series database.

## Examples Overview

### BasicOpenGeminiExample

A complete example showing:

- Creating a data stream with sensor readings
- Converting data to OpenGemini's LineProtocol format
- Configuring and using the OpenGemini sink
- Handling checkpointing and fault tolerance

## Important Notes

### Deprecated API Usage

⚠️ **Note**: This example uses the deprecated `SourceFunction` API for simplicity and demonstration purposes. For production environments, please use the modern Flink Source API:

- Use `Source` interface instead of `SourceFunction`
- Use `env.fromSource()` instead of `env.addSource()`
- Consider using `DataGeneratorSource` or custom `Source` implementations

## Important Notes

### Deprecated API Usage

⚠️ **Warning**: This example uses the deprecated `SourceFunction` API for simplicity and demonstration purposes. The deprecation warnings during compilation are expected.

**For production environments**, please use the modern Flink Source API:

- Use `Source` interface instead of `SourceFunction`
- Use `env.fromSource()` instead of `env.addSource()`
- Consider using `DataGeneratorSource` or implement custom `Source` classes

**Migration example**:

```java
// Deprecated (used in this example for simplicity):
env.addSource(new SensorDataSource())

// Modern approach (recommended for production):
env.fromSource(
    DataGeneratorSource.<SensorReading>builder()
        .setGeneratorFunction(new SensorGeneratorFunction())
        .setMaxRecordsPerSecond(100)
        .build(),
    WatermarkStrategy.noWatermarks(),
    "Modern Sensor Source"
)
```

### Dependency Configuration Disclaimer

⚠️ **Note**: The Maven dependencies in `pom.xml` are provided **for reference only** and may not represent the optimal configuration for your specific use case. Please review and adjust dependencies according to your:

- Target Flink version compatibility
- Deployment environment (standalone, YARN, Kubernetes, etc.)
- Performance and memory requirements
- Security and compliance constraints

**For production deployments**, consider:

- Using `<scope>provided</scope>` for Flink dependencies when deploying to Flink clusters
- Implementing proper dependency management strategies
- Following your organization's Maven dependency guidelines
- Performance testing with your actual data volumes

## Prerequisites

### 1. OpenGemini Database Server

You must have OpenGemini server running before executing the examples.

#### Option A: Download and Run Locally

#### Option B: Using Docker


### 2. Development Environment

- Java 8 or higher
- Apache Maven 3.6 or higher
- Apache Flink 1.18.1 (automatically handled by Maven dependencies)

## Running the Examples
You will need to construct a Maven project to compile and run the source files in this directory.
The Maven project should be workable with only limited editing as tested before.


`BasicOpenGeminiSinkExample.java` is the main class to run, which should be put in `src/main/java/` directory of the Maven project.

`log4j2.xml` is the logging configuration file, which should be put in resources directory of the Maven project.

`pom.xml` is the Maven configuration file, which should be put in the root directory of the Maven project.
Please rename files and make modifications as needed.

### Method 1: Using Maven (Recommended)
### Method 2: Using IDE
### Method 3: Creating Fat JAR for Cluster Deployment


## What the Example Does

The `BasicOpenGeminiSinkExample` demonstrates a complete data pipeline:

1. **Data Generation**: Creates a simple data source that generates sensor readings
2. **Data Transformation**: Converts Java objects to OpenGemini's LineProtocol format
3. **Data Ingestion**: Writes data to OpenGemini using the sink connector
4. **Fault Tolerance**: Implements checkpointing for reliable data processing

### Sample Data Format

The example generates sensor readings that are converted to LineProtocol format:

```
sensor_readings,sensor_id=sensor_1,location=room_1 temperature=23.45,humidity=52.3 1640995200000000000
sensor_readings,sensor_id=sensor_2,location=room_2 temperature=19.87,humidity=48.1 1640995201000000000
sensor_readings,sensor_id=sensor_3,location=outdoor temperature=25.12,humidity=45.7 1640995202000000000
```

## Verifying Results

### Using HTTP API (Recommended)

Query the data that was written to OpenGemini:

```bash
curl -G "http://localhost:8086/query" \
  --data-urlencode "db=example_db" \
  --data-urlencode "q=SELECT * FROM sensor_readings ORDER BY time DESC LIMIT 5"
```

Expected response:

```json
{
  "results": [
    {
      "series": [
        {
          "name": "sensor_readings",
          "columns": ["time", "humidity", "location", "sensor_id", "temperature"],
          "values": [
            ["2025-09-23T03:23:18Z", 52.3, "room_1", "sensor_1", 23.45],
            ["2025-09-23T03:23:17Z", 48.1, "room_2", "sensor_2", 19.87],
            ["2025-09-23T03:23:16Z", 45.7, "outdoor", "sensor_3", 25.12]
          ]
        }
      ]
    }
  ]
}
```

### Using OpenGemini CLI (If Available)

```bash
# Connect to OpenGemini
opengemini-cli -host localhost -port 8086

# Switch to the database
USE example_db

# List all measurements
SHOW MEASUREMENTS

# Query recent data
SELECT * FROM sensor_readings ORDER BY time DESC LIMIT 10

# Query specific sensor data
SELECT * FROM sensor_readings WHERE sensor_id='sensor_1' ORDER BY time DESC LIMIT 5
```

### Checking Database Creation

```bash
# Verify that the database was created
curl -G "http://localhost:8086/query" \
  --data-urlencode "q=SHOW DATABASES"
```

## Configuration Options

You can customize the example by modifying the sink configuration in `BasicOpenGeminiSinkExample.java`:

```java
OpenGeminiSinkConfiguration<SensorReading> sinkConfig = 
    OpenGeminiSinkConfiguration.<SensorReading>builder()
        .setHost("your-opengemini-host")     // Default: localhost
        .setPort(8086)                       // Default: 8086
        .setDatabase("your_database")        // Default: example_db
        .setMeasurement("your_measurement")  // Default: sensor_readings
        .setBatchSize(2000)                  // Default: 1000
        .setFlushInterval(3000, TimeUnit.MILLISECONDS)  // Default: 5000ms
        .setMaxRetries(5)                    // Default: 3
        .build();
```

## Expected Output

When running the example, you should see output similar to:

```
11:22:53 [INFO] BasicOpenGeminiSinkExample - Starting OpenGemini Basic Example...
11:22:57 [INFO] org.apache.flink.runtime.executiongraph.ExecutionGraph - Job OpenGemini Basic Example switched from state CREATED to RUNNING.
11:22:58 [INFO] org.opengemini.flink.sink.OpenGeminiSink - Created database: example_db
11:22:58 [INFO] org.opengemini.flink.sink.OpenGeminiSink - OpenGeminiSink initialized with host=localhost:8086, database=example_db, measurement=sensor_readings, batchSize=1000, flushInterval=5000ms
11:23:18 [INFO] org.opengemini.flink.sink.OpenGeminiSink - Completed checkpoint 1 with stats: points=1, batches=1, errors=0
```

## Troubleshooting

### Connection Refused Error

**Problem**: `java.net.ConnectException: Connection refused` **Solution**: Ensure OpenGemini server is running and accessible at `localhost:8086`

### Database Creation Issues

**Problem**: Database creation fails **Solution**: Check OpenGemini server logs and ensure sufficient permissions


### No Data Visible

**Problem**: Program runs but no data appears in queries **Solutions**:

- Check if OpenGemini server is running: `curl http://localhost:8086/ping`
- Verify database creation: `curl -G "http://localhost:8086/query" --data-urlencode "q=SHOW DATABASES"`
- Check for error logs in the Flink output

## Performance Tuning

For high-throughput scenarios, consider adjusting:

- **Batch Size**: Larger batches reduce network overhead
- **Flush Interval**: Longer intervals improve throughput but increase latency
- **Parallelism**: Match your system's CPU cores
- **Checkpointing**: Longer intervals reduce overhead

Example configuration for high throughput:

```java
.setBatchSize(5000)
.setFlushInterval(10000, TimeUnit.MILLISECONDS)
.setParallelism(4)
```