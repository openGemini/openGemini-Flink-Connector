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

import org.jetbrains.annotations.NotNull;

import io.opengemini.client.api.*;

/**
 * Factory class for creating OpenGemini client instances. Supports both standard and enhanced
 * client creation.
 */
public class OpenGeminiClientFactory {

    /**
     * Create an enhanced OpenGeminiClient instance with line protocol support.
     *
     * @param configuration the client configuration
     * @return a new EnhancedOpenGeminiClient instance
     * @throws OpenGeminiException if configuration is invalid
     */
    public static EnhancedOpenGeminiClient createEnhanced(@NotNull Configuration configuration)
            throws OpenGeminiException {
        validateConfiguration(configuration);
        return new EnhancedOpenGeminiClient(configuration);
    }

    /**
     * Validate the configuration before creating a client.
     *
     * @param configuration the configuration to validate
     * @throws OpenGeminiException if configuration is invalid
     */
    private static void validateConfiguration(@NotNull Configuration configuration)
            throws OpenGeminiException {
        // Validate addresses
        if (configuration.getAddresses() == null || configuration.getAddresses().isEmpty()) {
            throw new OpenGeminiException("must have at least one address");
        }

        // Validate auth configuration
        AuthConfig authConfig = configuration.getAuthConfig();
        if (authConfig != null) {
            validateAuthConfig(authConfig);
        }

        // Validate batch configuration
        BatchConfig batchConfig = configuration.getBatchConfig();
        if (batchConfig != null) {
            validateBatchConfig(batchConfig);
        }
    }

    /**
     * Validate authentication configuration.
     *
     * @param authConfig the authentication configuration to validate
     * @throws OpenGeminiException if auth configuration is invalid
     */
    private static void validateAuthConfig(@NotNull AuthConfig authConfig)
            throws OpenGeminiException {
        if (authConfig.getAuthType() == AuthType.TOKEN) {
            if (authConfig.getToken() == null || authConfig.getToken().isEmpty()) {
                throw new OpenGeminiException("invalid auth config due to empty token");
            }
        }

        if (authConfig.getAuthType() == AuthType.PASSWORD) {
            if (authConfig.getUsername() == null || authConfig.getUsername().isEmpty()) {
                throw new OpenGeminiException("invalid auth config due to empty username");
            }
            if (authConfig.getPassword() == null || authConfig.getPassword().length == 0) {
                throw new OpenGeminiException("invalid auth config due to empty password");
            }
        }
    }

    /**
     * Validate batch configuration.
     *
     * @param batchConfig the batch configuration to validate
     * @throws OpenGeminiException if batch configuration is invalid
     */
    private static void validateBatchConfig(@NotNull BatchConfig batchConfig)
            throws OpenGeminiException {
        if (batchConfig.getBatchInterval() <= 0) {
            throw new OpenGeminiException("batch enabled, batch interval must be great than 0");
        }
        if (batchConfig.getBatchSize() <= 0) {
            throw new OpenGeminiException("batch enabled, batch size must be great than 0");
        }
    }
}
