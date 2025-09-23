# CONTRIBUTING.md

```markdown
# Contributing to OpenGemini Flink Connector

Thank you for your interest in contributing to the OpenGemini Flink Connector project! This guide will help you get started with the development process.

## Table of Contents
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Code Standards](#code-standards)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Issue Reporting](#issue-reporting)
- [Community Guidelines](#community-guidelines)

## Development Setup

### Prerequisites
- Java 8 or later
- Maven 3.6+
- Apache Flink 1.18.1 (for testing, may also work well with other versions but needs testing)
- OpenGemini database (for integration tests)
- Git

### Environment Setup

1. **Fork and Clone the Repository**
   ```bash
   git clone https://github.com/YOUR-USERNAME/openGemini-Flink-Connector.git
   cd openGemini-Flink-Connector
   ```

2. **Install Dependencies**
   ```bash
   mvn clean install
   ```

3. **Verify Setup**
   ```bash
   mvn compile
   mvn test
   ```

## Development Workflow

### Standard Development Cycle

1. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Development Phase**
   ```bash
   # Make your code changes

   # Compile early and often
   mvn compile
   ```

3. **Code Quality Checks**
   ```bash
   # Apply code formatting
   mvn spotless:apply

   # Add license headers
   mvn license:format
   ```

4. **Testing**
   ```bash
   # Run unit tests
   mvn test
   ```

5. **Final Verification**
   ```bash
   # Clean build with all checks
   mvn clean verify
   ```

6. **Commit and Push**
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   git push origin feature/your-feature-name
   ```

### Quick Commands for Development

```bash
# Quick compilation check
mvn compile

# Format code without full build
mvn  license:format spotless:apply

# Run specific test class
mvn test -Dtest=OpenGeminiSinkTest

# Skip tests during development (use sparingly)
mvn compile -DskipTests

# Check code quality without fixing
mvn spotless:check license:check
```

## Code Standards

### Java Code Style

- **Formatting**: Use Spotless for automatic formatting
- **Naming**: Use camelCase for methods and variables, PascalCase for classes
- **Documentation**: All public methods must have JavaDoc
- **Line Length**: Maximum 120 characters per line

### Code Structure

```java
// File header with license (applied by mvn license:format)

package org.opengemini.flink.sink;

import statements (ordered automatically by Spotless)

/**
 * Class-level JavaDoc explaining purpose and usage
 */
public class YourClass {
    // Static constants first
    private static final String CONSTANT_NAME = "value";

    // Instance fields
    private final String requiredField;
    private transient OptionalType optionalField;

    // Constructor(s)
    public YourClass(String requiredField) {
        this.requiredField = requiredField;
    }

    // Public methods
    /**
     * Method description
     * @param parameter description
     * @return description
     */
    public ReturnType publicMethod(ParameterType parameter) {
        // Implementation
    }

    // Private methods
    private void privateMethod() {
        // Implementation
    }
}
```

### Performance Guidelines

- Avoid object creation in hot paths
- Use thread-safe collections appropriately
- Consider memory allocation patterns
- Profile performance-critical code paths

### Error Handling

```java
// Good: Specific exception handling
try {
    client.write(data);
} catch (OpenGeminiException e) {
    log.error("Failed to write to OpenGemini: {}", e.getMessage(), e);
    handleOpenGeminiError(e);
} catch (IOException e) {
    log.error("Network error writing to OpenGemini", e);
    handleNetworkError(e);
}

// Bad: Generic exception catching
try {
    client.write(data);
} catch (Exception e) {
    // Too generic
}
```

## Testing Guidelines

### Unit Tests

- **Coverage**: Aim for >80% line coverage. Compilation will fail if below 70%.

```java
@Test
public void openGeminiSink_invokeWithValidData_shouldBufferData() {
    // Given
    OpenGeminiSink<TestData> sink = createTestSink();
    TestData testData = new TestData("sensor1", 25.0);

    // When
    sink.invoke(testData, mockContext);

    // Then
    assertThat(sink.getCurrentBatchSize()).isEqualTo(1);
}
```

### Integration Tests

- Use TestContainers for OpenGemini when possible
- Test with real Flink mini cluster
- Verify end-to-end data flow

### Test Data

- Use builders for complex test objects
- Keep test data realistic but minimal
- Store large test datasets in resources


## Development Tips

### Debugging

```java
// Use structured logging
log.debug("Processing batch: size={}, measurement={}",
    batchSize, measurement);

// Add metrics for debugging
metricGroup.gauge("debug.batchSize", () -> currentBatchSize);
```

### Performance Testing
Please refer to https://github.com/PeterZh6/opengemini-flink-connector-benchmark for performance testing guidelines and tools.
You may also write your own performance tests as the benchmark project does not cover all scenarios and may not be maintained.


Thank you for contributing to the OpenGemini Flink Connector! Your contributions help make real-time time-series data processing more accessible and reliable.
```
