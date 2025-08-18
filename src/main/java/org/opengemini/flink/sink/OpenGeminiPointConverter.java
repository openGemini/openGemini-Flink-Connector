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
package org.opengemini.flink.sink;

import java.io.Serializable;

import io.opengemini.client.api.Point;

/**
 * Enhanced converter interface for converting objects to OpenGemini Points. This provides better
 * integration with the OpenGemini client.
 *
 * @param <T> The type of object to convert
 */
public interface OpenGeminiPointConverter<T> extends Serializable {
    /**
     * Convert an object to an OpenGemini Point.
     *
     * @param value The value to convert
     * @param measurement The measurement name
     * @return The converted Point, or null if the value should be skipped
     */
    Point convert(T value, String measurement) throws Exception;
}
