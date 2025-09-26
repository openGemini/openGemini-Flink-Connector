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
package org.opengemini.flink.utils;

import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import io.opengemini.client.api.Configuration;
import io.opengemini.client.api.OpenGeminiException;
import io.opengemini.client.impl.OpenGeminiClient;

public class EnhancedOpenGeminiClient extends OpenGeminiClient {
    public EnhancedOpenGeminiClient(@NotNull Configuration conf) {
        super(conf);
    }

    /**
     * Write data to database using line protocol string.
     *
     * @param database the name of the database
     * @param lineProtocol the line protocol string to write
     * @return a CompletableFuture that completes when the write is done
     */
    public CompletableFuture<Void> writeLineProtocol(String database, String lineProtocol) {
        return writeLineProtocol(database, null, lineProtocol);
    }

    /**
     * Write data to database using line protocol string with specified retention policy.
     *
     * @param database the name of the database
     * @param retentionPolicy the name of the retention policy (optional)
     * @param lineProtocol the line protocol string to write
     * @return a CompletableFuture that completes when the write is done
     */
    public CompletableFuture<Void> writeLineProtocol(
            String database, String retentionPolicy, String lineProtocol) {
        if (StringUtils.isBlank(database)) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(
                    new IllegalArgumentException("Database name cannot be null or empty"));
            return future;
        }

        if (StringUtils.isEmpty(lineProtocol)) {
            return CompletableFuture.completedFuture(null);
        }

        return executeWrite(database, retentionPolicy, lineProtocol);
    }

    /**
     * Write multiple line protocol strings to database.
     *
     * @param database the name of the database
     * @param lineProtocols list of line protocol strings to write
     * @return a CompletableFuture that completes when the write is done
     * @throws OpenGeminiException if the write fails
     */
    public CompletableFuture<Void> writeLineProtocols(String database, List<String> lineProtocols) {
        return writeLineProtocols(database, null, lineProtocols);
    }

    /**
     * Write multiple line protocol strings to database with specified retention policy.
     *
     * @param database the name of the database
     * @param retentionPolicy the name of the retention policy (optional)
     * @param lineProtocols list of line protocol strings to write
     * @return a CompletableFuture that completes when the write is done
     */
    public CompletableFuture<Void> writeLineProtocols(
            String database, String retentionPolicy, List<String> lineProtocols) {
        if (StringUtils.isBlank(database)) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(
                    new IllegalArgumentException("Database name cannot be null or empty"));
            return future;
        }

        if (lineProtocols == null || lineProtocols.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        StringJoiner sj = new StringJoiner("\n");
        for (String lineProtocol : lineProtocols) {
            if (StringUtils.isNotEmpty(lineProtocol)) {
                // Trim each line to remove any extra whitespace
                sj.add(lineProtocol.trim());
            }
        }

        String body = sj.toString();
        if (StringUtils.isEmpty(body)) {
            return CompletableFuture.completedFuture(null);
        }

        return executeWrite(database, retentionPolicy, body);
    }

    /**
     * Write batch data to database using a single line protocol string containing multiple lines.
     * Each line in the string should represent a separate data point.
     *
     * @param database the name of the database
     * @param retentionPolicy the name of the retention policy (optional)
     * @param batchLineProtocol the batch line protocol string (multiple lines separated by \n)
     * @return a CompletableFuture that completes when the write is done
     */
    public CompletableFuture<Void> writeBatchLineProtocol(
            String database, String retentionPolicy, String batchLineProtocol) {
        if (StringUtils.isBlank(database)) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(
                    new IllegalArgumentException("Database name cannot be null or empty"));
            return future;
        }

        if (StringUtils.isEmpty(batchLineProtocol)) {
            return CompletableFuture.completedFuture(null);
        }

        // Validate and clean up the batch line protocol
        String[] lines = batchLineProtocol.split("\n");
        StringJoiner cleanedBatch = new StringJoiner("\n");
        for (String line : lines) {
            String trimmedLine = line.trim();
            if (StringUtils.isNotEmpty(trimmedLine)) {
                cleanedBatch.add(trimmedLine);
            }
        }

        String cleanedBody = cleanedBatch.toString();
        if (StringUtils.isEmpty(cleanedBody)) {
            return CompletableFuture.completedFuture(null);
        }

        return executeWrite(database, retentionPolicy, cleanedBody);
    }

    /**
     * Write batch data to database using a single line protocol string containing multiple lines.
     * Uses the default retention policy.
     *
     * @param database the name of the database
     * @param batchLineProtocol the batch line protocol string (multiple lines separated by \n)
     * @return a CompletableFuture that completes when the write is done
     */
    public CompletableFuture<Void> writeBatchLineProtocol(
            String database, String batchLineProtocol) {
        return writeBatchLineProtocol(database, null, batchLineProtocol);
    }

    @Override
    public String toString() {
        return "EnhancedOpenGeminiClient{" + "httpEngine=" + conf.getHttpConfig().engine() + '}';
    }
}
