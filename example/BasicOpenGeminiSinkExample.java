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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.function.SerializableFunction;
import org.opengemini.flink.sink.*;

import lombok.extern.slf4j.Slf4j;

/**
 * A complete example showing how to use OpenGemini Flink Sink Connector.
 *
 * <p>This example demonstrates: 1. Creating a simple data source 2. Converting data to LineProtocol
 * format 3. Configuring and using OpenGemini sink
 *
 * <p>Prerequisites: - OpenGemini server running on localhost:8086 - Database 'example_db' created
 * (or it will be auto-created)
 *
 * <p>To run this example: mvn exec:java
 * -Dexec.mainClass="org.opengemini.examples.BasicOpenGeminiExample"
 */
@Slf4j
public class BasicOpenGeminiExample {

    public static void main(String[] args) throws Exception {
        // Setup Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(30000); // checkpoint every 30 seconds

        // Create a data source that generates sensor readings
        DataStream<SensorReading> sensorData =
                env.addSource(new SensorDataSource()).setParallelism(1).name("Sensor Data Source");

        // Create LineProtocol converter
        OpenGeminiLineProtocolConverter<SensorReading> converter = createLineProtocolConverter();

        // Configure OpenGemini sink
        OpenGeminiSinkConfiguration<SensorReading> sinkConfig =
                OpenGeminiSinkConfiguration.<SensorReading>builder()
                        .setHost("localhost")
                        .setPort(8086)
                        .setDatabase("example_db")
                        .setMeasurement("sensor_readings")
                        .setBatchSize(1000) // batch 1000 records before sending
                        .setFlushInterval(5000, TimeUnit.MILLISECONDS) // or flush every 5 seconds
                        .setMaxRetries(3)
                        .setConverter(converter)
                        .build();

        // Add OpenGemini sink to the data stream
        sensorData
                .addSink(new OpenGeminiSink<>(sinkConfig))
                .setParallelism(2)
                .name("OpenGemini Sink");
        log.info("Starting OpenGemini Basic Example...");
        // Execute the job
        env.execute("OpenGemini Basic Example");
    }

    /**
     * Creates a LineProtocol converter for SensorReading data. This converter defines how to map
     * Java objects to OpenGemini's LineProtocol format.
     */
    private static OpenGeminiLineProtocolConverter<SensorReading> createLineProtocolConverter() {
        SimpleOpenGeminiLineProtocolConverter<SensorReading> protocolConverter =
                SimpleOpenGeminiLineProtocolConverter.<SensorReading>builder()
                        // Add tags (indexed fields)
                        .addTag("sensor_id", SensorReading::getSensorId)
                        .addTag(
                                "location",
                                (SerializableFunction<SensorReading, String>)
                                        SensorReading::getLocation)
                        // Add fields (actual data values)
                        .addField(
                                "temperature",
                                (SerializableFunction<SensorReading, Object>)
                                        SensorReading::getTemperature)
                        .addField(
                                "humidity",
                                (SerializableFunction<SensorReading, Object>)
                                        SensorReading::getHumidity)
                        // Set timestamp
                        .withTimestampMillis(
                                (SerializableFunction<SensorReading, Long>)
                                        SensorReading::getTimestamp)
                        .build();

        return new OpenGeminiLineProtocolConverter<SensorReading>() {
            @Override
            public String convertToLineProtocol(SensorReading data, String measurement) {
                return protocolConverter.convertToLineProtocol(data, measurement);
            }
        };
    }

    /** Simple data source that generates sensor readings every second. */
    private static class SensorDataSource implements SourceFunction<SensorReading> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] locations = {"room_1", "room_2", "room_3", "outdoor"};

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            int counter = 0;

            while (running) {
                // Generate sample sensor reading
                SensorReading reading =
                        new SensorReading(
                                "sensor_" + (counter % 10 + 1), // sensor_1 to sensor_10
                                locations[counter % locations.length],
                                20.0 + random.nextGaussian() * 5, // temperature around 20°C
                                50.0 + random.nextGaussian() * 10, // humidity around 50%
                                System.currentTimeMillis());

                ctx.collect(reading);
                counter++;

                // Wait 1 second between readings
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** Simple POJO representing a sensor reading. */
    public static class SensorReading {
        private final String sensorId;
        private final String location;
        private final double temperature;
        private final double humidity;
        private final long timestamp;

        public SensorReading(
                String sensorId,
                String location,
                double temperature,
                double humidity,
                long timestamp) {
            this.sensorId = sensorId;
            this.location = location;
            this.temperature = temperature;
            this.humidity = humidity;
            this.timestamp = timestamp;
        }

        // Getters
        public String getSensorId() {
            return sensorId;
        }

        public String getLocation() {
            return location;
        }

        public double getTemperature() {
            return temperature;
        }

        public double getHumidity() {
            return humidity;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return String.format(
                    "SensorReading{sensor='%s', location='%s', temp=%.2f, humidity=%.2f, ts=%d}",
                    sensorId, location, temperature, humidity, timestamp);
        }
    }
}