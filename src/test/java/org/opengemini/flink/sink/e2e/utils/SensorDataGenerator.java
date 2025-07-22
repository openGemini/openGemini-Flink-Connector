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
package org.opengemini.flink.sink.e2e.utils;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.opengemini.flink.sink.e2e.model.SensorData;

/** Test data generator for sensor data. */
public class SensorDataGenerator implements SourceFunction<SensorData> {
    private static final String[] LOCATIONS = {"room1", "room2", "room3", "hall", "outdoor"};
    private static final String[] SENSOR_IDS = {
        "sensor1", "sensor2", "sensor3", "sensor4", "sensor5"
    };

    private final long numberOfRecords;
    private final long delayBetweenRecordsMs;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private long recordsGenerated = 0;

    public SensorDataGenerator(long numberOfRecords, long delayBetweenRecordsMs) {
        this.numberOfRecords = numberOfRecords;
        this.delayBetweenRecordsMs = delayBetweenRecordsMs;
    }

    @Override
    public void run(SourceContext<SensorData> ctx) throws Exception {
        Random random = new Random();
        long baseTimestamp = System.currentTimeMillis();

        while (running.get() && recordsGenerated < numberOfRecords) {
            String sensorId = SENSOR_IDS[random.nextInt(SENSOR_IDS.length)];
            String location = LOCATIONS[random.nextInt(LOCATIONS.length)];
            double temperature = 20 + random.nextDouble() * 10; // 20-30°C
            double humidity = 40 + random.nextDouble() * 40; // 40-80%
            double pressure = 1000 + random.nextDouble() * 50; // 1000-1050 hPa

            // ensures that timestamps are unique so that entry counts can be verified more easily
            long timestamp = baseTimestamp + recordsGenerated;
            SensorData data =
                    new SensorData(sensorId, location, temperature, humidity, pressure, timestamp);
            ctx.collect(data);
            recordsGenerated++;

            if (delayBetweenRecordsMs > 0) {
                Thread.sleep(delayBetweenRecordsMs);
            }
        }
    }

    @Override
    public void cancel() {
        running.set(false);
    }

    public long getRecordsGenerated() {
        return recordsGenerated;
    }
}
