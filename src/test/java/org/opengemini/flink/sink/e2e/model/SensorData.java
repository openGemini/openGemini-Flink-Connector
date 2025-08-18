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
package org.opengemini.flink.sink.e2e.model;

import java.io.Serializable;

/** Test data model representing sensor data. */
public class SensorData implements Serializable {
    private final String sensorId;
    private final String location;
    private final double temperature;
    private final double humidity;
    private final double pressure;
    private final long timestamp;

    public SensorData(
            String sensorId,
            String location,
            double temperature,
            double humidity,
            double pressure,
            long timestamp) {
        this.sensorId = sensorId;
        this.location = location;
        this.temperature = temperature;
        this.humidity = humidity;
        this.pressure = pressure;
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

    public double getPressure() {
        return pressure;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format(
                "SensorData[id=%s, loc=%s, temp=%.2f, hum=%.2f, press=%.2f, ts=%d]",
                sensorId, location, temperature, humidity, pressure, timestamp);
    }
}
