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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import io.github.openfacade.http.HttpClientConfig;
import io.opengemini.client.api.Address;
import io.opengemini.client.api.Point;
import io.opengemini.client.impl.OpenGeminiClient;
import io.opengemini.client.impl.OpenGeminiClientFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * An Apache Flink sink connector for OpenGemini using the official OpenGemini client. This
 * implementation provides efficient batch writing with exactly-once semantics.
 *
 * @param <T> The type of elements handled by this sink
 */
@Slf4j
public class OpenGeminiSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    // Configuration
    private final OpenGeminiSinkConfiguration<T> configuration;

    // TODO: decouple converter from configuration
    // Converter for converting input values to OpenGemini Points
    private OpenGeminiPointConverter<T> converter;

    // Runtime state
    private transient OpenGeminiClient client;
    private transient List<Point> currentBatch;
    private transient long lastFlushTime;
    // TODO: support Flink metrics
    private transient AtomicLong totalBytesWritten;
    private transient AtomicLong totalPointsWritten;
    private transient AtomicLong errorCount;
    private transient AtomicLong batchesWritten;

    /**
     * Creates a new OpenGeminiSink with the specified configuration.
     *
     * @param configuration The sink configuration
     */
    public OpenGeminiSink(OpenGeminiSinkConfiguration<T> configuration) {
        this.configuration = configuration;
    }

    // TODO: decouple converter from configuration
    public OpenGeminiSink(
            OpenGeminiSinkConfiguration<T> configuration, OpenGeminiPointConverter<T> converter) {
        this.configuration = configuration;
        this.converter = converter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Initialize OpenGemini client
        HttpClientConfig.Builder httpConfigBuilder =
                new HttpClientConfig.Builder()
                        .connectTimeout(configuration.getConnectionTimeout())
                        .timeout(configuration.getRequestTimeout());

        // Set authentication if provided
        if (configuration.hasAuthentication()) {
            httpConfigBuilder.addRequestFilter(
                    new io.github.openfacade.http.BasicAuthRequestFilter(
                            configuration.getUsername(), configuration.getPassword()));
        }

        HttpClientConfig httpConfig = httpConfigBuilder.build();

        io.opengemini.client.api.Configuration clientConfiguration =
                io.opengemini.client.api.Configuration.builder()
                        .addresses(
                                Collections.singletonList(
                                        new Address(
                                                configuration.getHost(), configuration.getPort())))
                        .httpConfig(httpConfig)
                        .build();

        this.client = OpenGeminiClientFactory.create(clientConfiguration);

        // Initialize runtime components
        totalBytesWritten = new AtomicLong(0);
        totalPointsWritten = new AtomicLong(0);
        errorCount = new AtomicLong(0);
        batchesWritten = new AtomicLong(0);

        // Ensure database exists
        try {
            client.createDatabase(configuration.getDatabase()).get(5, TimeUnit.SECONDS);
            log.info("Created database: {}", configuration.getDatabase());
        } catch (Exception e) {
            // Database might already exist, which is fine
            log.debug(
                    "Database {} might already exist: {}",
                    configuration.getDatabase(),
                    e.getMessage());
        }

        log.info(
                "OpenGeminiSink initialized with host={}:{}, database={}, measurement={}, batchSize={}, flushInterval={}ms",
                configuration.getHost(),
                configuration.getPort(),
                configuration.getDatabase(),
                configuration.getMeasurement(),
                configuration.getBatchSize(),
                configuration.getFlushIntervalMillis());
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (value == null) return;

        Point point = configuration.getConverter().convert(value, configuration.getMeasurement());
        if (point == null) {
            log.debug("Converter returned null for value: {}", value);
            return;
        }

        currentBatch.add(point);

        if (shouldFlush()) {
            flush();
        }
    }

    private boolean shouldFlush() {
        boolean batchFull = currentBatch.size() >= configuration.getBatchSize();
        boolean timeoutReached =
                configuration.getFlushIntervalMillis() > 0
                        && (System.currentTimeMillis() - lastFlushTime)
                                >= configuration.getFlushIntervalMillis();

        return batchFull || (timeoutReached && !currentBatch.isEmpty());
    }

    private void flush() throws Exception {
        if (currentBatch == null || currentBatch.isEmpty()) {
            return;
        }

        List<Point> batchToWrite = new ArrayList<>(currentBatch);
        currentBatch.clear();

        try {
            writeBatch(batchToWrite);
            lastFlushTime = System.currentTimeMillis();
        } catch (Exception e) {
            log.error("Error writing batch to OpenGemini: {}", e.getMessage(), e);
            errorCount.incrementAndGet();
            // Re-add points to current batch for retry/checkpointing
            currentBatch.addAll(batchToWrite);
            // rethrow to signal failure
            throw e;
        }
    }

    /**
     * Writes a batch of points to OpenGemini. This method handles retries.
     *
     * @param points
     * @throws Exception
     */
    private void writeBatch(List<Point> points) throws Exception {
        if (points.isEmpty()) return;

        log.debug(
                "Writing {} points to {}.{}",
                points.size(),
                configuration.getDatabase(),
                configuration.getMeasurement());

        int retries = 0;
        Exception lastException = null;

        while (retries <= configuration.getMaxRetries()) {
            try {
                long startTime = System.currentTimeMillis();

                // use async API but wait synchronously for completion
                CompletableFuture<Void> future = client.write(configuration.getDatabase(), points);
                future.get(configuration.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS);

                long writeTime = System.currentTimeMillis() - startTime;

                // update statistics
                totalPointsWritten.addAndGet(points.size());
                batchesWritten.incrementAndGet();
                long estimatedBytes = estimatePointsSize(points);
                totalBytesWritten.addAndGet(estimatedBytes);

                log.debug(
                        "Successfully wrote batch with {} points in {}ms",
                        points.size(),
                        writeTime);
                // success
                return;

            } catch (Exception e) {
                lastException = e;
                log.warn(
                        "Error writing to OpenGemini (attempt {}/{}): {}",
                        retries + 1,
                        configuration.getMaxRetries() + 1,
                        e.getMessage());
                errorCount.incrementAndGet();
                retries++;

                if (retries <= configuration.getMaxRetries()) {
                    // exponential backoff
                    long backoffMs = Math.min(1000L * (1L << retries), 10000L);
                    Thread.sleep(backoffMs);
                }
            }
        }

        throw new RuntimeException(
                "Failed to write batch after " + configuration.getMaxRetries() + " retries",
                lastException);
    }

    /**
     * Used to calculate point size for Flink Metrics
     *
     * @param points
     * @return
     */
    private long estimatePointsSize(List<Point> points) {
        long totalSize = 0;
        for (Point point : points) {
            // Estimate based on line protocol size
            String lineProtocol = point.lineProtocol();
            totalSize += lineProtocol.getBytes().length;
        }
        return totalSize;
    }

    /**
     * Called to close the sink and perform any final cleanup. This will flush any remaining points
     */
    @Override
    public void close() throws Exception {
        log.info("Closing OpenGeminiSink...");

        try {
            // write any remaining points
            flush();
        } catch (Exception e) {
            log.error("Error during final flush", e);
        }

        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Error closing OpenGemini client", e);
                throw new RuntimeException("Failed to close OpenGemini client", e);
            }
        }

        log.info(
                "OpenGeminiSink closed. Final stats: totalPoints={}, batches={}, errors={}",
                totalPointsWritten.get(),
                batchesWritten.get(),
                errorCount.get());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // TODO: implement state snapshotting
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
            throws Exception {
        // TODO: implement state initialization
        currentBatch = new ArrayList<>(configuration.getBatchSize());
    }
}
