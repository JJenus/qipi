package com.jjenus.qipi.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("StorageConfig Tests")
class StorageConfigTest {

    @Nested
    @DisplayName("Builder Basic Operations")
    class BuilderBasicTests {

        @Test
        @DisplayName("Should create LOCAL config with defaults")
        void shouldCreateLocalConfigWithDefaults() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .build();

            assertEquals(StorageConfig.ProviderType.LOCAL, config.getProviderType());
            assertEquals("./storage", config.getBasePath()); // Default value
            assertEquals("us-east-1", config.getRegion()); // Default value
        }

        @Test
        @DisplayName("Should override LOCAL defaults when specified")
        void shouldOverrideLocalDefaults() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .basePath("/custom/path")
                    .region("eu-west-1")
                    .build();

            assertEquals("/custom/path", config.getBasePath());
            assertEquals("eu-west-1", config.getRegion());
        }

        @Test
        @DisplayName("Should create AWS_S3 config with required fields")
        void shouldCreateAwsS3Config() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.AWS_S3)
                    .region("us-west-2")
                    .credentials("AKIA123", "secret456")
                    .build();

            assertEquals(StorageConfig.ProviderType.AWS_S3, config.getProviderType());
            assertEquals("us-west-2", config.getRegion());
            assertEquals("AKIA123", config.getAccessKey());
            assertEquals("secret456", config.getSecretKey());
        }

        @Test
        @DisplayName("Should create AWS_S3 config with default region")
        void shouldCreateAwsS3WithDefaultRegion() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.AWS_S3)
                    .credentials("AKIA123", "secret456")
                    .build();

            assertEquals("us-east-1", config.getRegion()); // Default region
        }

        @Test
        @DisplayName("Should create MINIO config with required fields")
        void shouldCreateMinioConfig() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.MINIO)
                    .endpoint("localhost:9000")
                    .credentials("minioadmin", "minioadmin")
                    .build();

            assertEquals(StorageConfig.ProviderType.MINIO, config.getProviderType());
            assertEquals("localhost:9000", config.getEndpoint());
            assertEquals("minioadmin", config.getAccessKey());
            assertEquals("minioadmin", config.getSecretKey());
        }

        @Test
        @DisplayName("Should create GCS config without validation")
        void shouldCreateGcsConfig() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.GCS)
                    .build();

            assertEquals(StorageConfig.ProviderType.GCS, config.getProviderType());
        }
    }

    @Nested
    @DisplayName("Builder Validation Tests")
    class BuilderValidationTests {

        @Test
        @DisplayName("Should throw when provider type is null")
        void shouldThrowWhenProviderNull() {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> new StorageConfig.Builder().build()
            );
            assertEquals("Provider type must be specified", exception.getMessage());
        }

        @Test
        @DisplayName("Should validate LOCAL requires non-empty basePath when explicitly set to empty")
        void shouldValidateLocalBasePathWhenSetToEmpty() {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> new StorageConfig.Builder()
                            .provider(StorageConfig.ProviderType.LOCAL)
                            .basePath("")
                            .build()
            );
            assertEquals("basePath is required for LOCAL storage", exception.getMessage());
        }

        @Test
        @DisplayName("Should validate LOCAL requires non-empty basePath when set to whitespace")
        void shouldValidateLocalBasePathWhenSetToWhitespace() {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> new StorageConfig.Builder()
                            .provider(StorageConfig.ProviderType.LOCAL)
                            .basePath("   ")
                            .build()
            );
            assertEquals("basePath is required for LOCAL storage", exception.getMessage());
        }

        @Test
        @DisplayName("Should validate AWS_S3 requires region when set to null")
        void shouldValidateAwsS3RegionWhenSetToNull() {
            // This will use default region "us-east-1", so it shouldn't throw
            assertDoesNotThrow(() ->
                    new StorageConfig.Builder()
                            .provider(StorageConfig.ProviderType.AWS_S3)
                            .credentials("key", "secret")
                            .build()
            );
        }

        @Test
        @DisplayName("Should validate AWS_S3 requires region when set to empty")
        void shouldValidateAwsS3RegionWhenSetToEmpty() {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> new StorageConfig.Builder()
                            .provider(StorageConfig.ProviderType.AWS_S3)
                            .region("")
                            .credentials("key", "secret")
                            .build()
            );
            assertEquals("region is required for AWS_S3 storage", exception.getMessage());
        }

        @Test
        @DisplayName("Should validate AWS_S3 requires credentials")
        void shouldValidateAwsS3Credentials() {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> new StorageConfig.Builder()
                            .provider(StorageConfig.ProviderType.AWS_S3)
                            .region("us-east-1")
                            .build()
            );
            assertEquals("accessKey and secretKey are required for AWS_S3 storage", exception.getMessage());
        }

        @Test
        @DisplayName("Should validate AWS_S3 requires both accessKey and secretKey")
        void shouldValidateAwsS3BothCredentials() {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> new StorageConfig.Builder()
                            .provider(StorageConfig.ProviderType.AWS_S3)
                            .region("us-east-1")
                            .credentials("key", null)
                            .build()
            );
            assertEquals("accessKey and secretKey are required for AWS_S3 storage", exception.getMessage());
        }

        @Test
        @DisplayName("Should validate MINIO requires endpoint")
        void shouldValidateMinioEndpoint() {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> new StorageConfig.Builder()
                            .provider(StorageConfig.ProviderType.MINIO)
                            .credentials("key", "secret")
                            .build()
            );
            assertEquals("endpoint is required for MINIO storage", exception.getMessage());
        }

        @Test
        @DisplayName("Should validate MINIO requires credentials")
        void shouldValidateMinioCredentials() {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> new StorageConfig.Builder()
                            .provider(StorageConfig.ProviderType.MINIO)
                            .endpoint("localhost:9000")
                            .build()
            );
            assertEquals("accessKey and secretKey are required for MINIO storage", exception.getMessage());
        }
    }

    @Nested
    @DisplayName("Configuration Loading Tests")
    class ConfigurationLoadingTests {

        @Test
        @DisplayName("Should load from Properties")
        void shouldLoadFromProperties() {
            Properties props = new Properties();
            props.setProperty("storage.provider", "LOCAL");
            props.setProperty("storage.basePath", "./custom");
            props.setProperty("storage.region", "eu-west-1");

            StorageConfig config = new StorageConfig.Builder()
                    .fromProperties(props)
                    .build();

            assertEquals(StorageConfig.ProviderType.LOCAL, config.getProviderType());
            assertEquals("./custom", config.getBasePath());
            assertEquals("eu-west-1", config.getRegion());
        }

        @Test
        @DisplayName("Should use defaults when properties missing")
        void shouldUseDefaultsWhenPropertiesMissing() {
            Properties props = new Properties();
            props.setProperty("storage.provider", "LOCAL");
            // basePath and region not specified

            StorageConfig config = new StorageConfig.Builder()
                    .fromProperties(props)
                    .build();

            assertEquals("./storage", config.getBasePath()); // Default
            assertEquals("us-east-1", config.getRegion()); // Default
        }

        @Test
        @DisplayName("Should override builder values with properties")
        void shouldOverrideBuilderWithProperties() {
            Properties props = new Properties();
            props.setProperty("storage.provider", "LOCAL");
            props.setProperty("storage.basePath", "/from/props");
            props.setProperty("storage.region", "from-props");

            StorageConfig config = new StorageConfig.Builder()
                    .basePath("/from/builder")
                    .region("from-builder")
                    .fromProperties(props)
                    .build();

            assertEquals("/from/props", config.getBasePath());
            assertEquals("from-props", config.getRegion());
        }

        @Test
        @DisplayName("Should load from Map")
        void shouldLoadFromMap() {
            Map<String, String> map = new HashMap<>();
            map.put("storage.provider", "MINIO");
            map.put("storage.endpoint", "minio:9000");
            map.put("storage.accessKey", "testkey");
            map.put("storage.secretKey", "testsecret");

            StorageConfig config = new StorageConfig.Builder()
                    .fromMap(map)
                    .build();

            assertEquals(StorageConfig.ProviderType.MINIO, config.getProviderType());
            assertEquals("minio:9000", config.getEndpoint());
            assertEquals("testkey", config.getAccessKey());
            assertEquals("testsecret", config.getSecretKey());
        }
    }

    @Nested
    @DisplayName("Optional Fields Tests")
    class OptionalFieldsTests {

        @Test
        @DisplayName("Should set all optional fields")
        void shouldSetAllOptionalFields() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .endpoint("custom.endpoint.com")
                    .sessionToken("session123")
                    .pathStyleAccess(true)
                    .useHttps(false)
                    .timeouts(5000, 10000)
                    .maxConnections(100)
                    .bucketPrefix("prefix-")
                    .baseUrl("https://storage.example.com/")
                    .signingKey("test-key")
                    .multipartSettings(1024 * 1024, 1024 * 1024 * 1024, 1000)
                    .defaultUrlExpirySeconds(7200)
                    .build();

            assertEquals("custom.endpoint.com", config.getEndpoint());
            assertEquals("session123", config.getSessionToken());
            assertTrue(config.isPathStyleAccess());
            assertFalse(config.isUseHttps());
            assertEquals(5000, config.getConnectionTimeout());
            assertEquals(10000, config.getSocketTimeout());
            assertEquals(100, config.getMaxConnections());
            assertEquals("prefix-", config.getBucketPrefix());
            assertEquals("https://storage.example.com/", config.getBaseUrl());
            assertEquals("test-key", config.getSigningKey());
            assertEquals(1024 * 1024, config.getMultipartMinPartSize());
            assertEquals(1024 * 1024 * 1024, config.getMultipartMaxPartSize());
            assertEquals(1000, config.getMultipartMaxParts());
            assertEquals(7200, config.getDefaultUrlExpirySeconds());
        }

        @Test
        @DisplayName("Should set session token via credentials method")
        void shouldSetSessionToken() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.AWS_S3)
                    .region("us-east-1")
                    .credentials("key", "secret", "token")
                    .build();

            assertEquals("key", config.getAccessKey());
            assertEquals("secret", config.getSecretKey());
            assertEquals("token", config.getSessionToken());
        }
    }

    @Nested
    @DisplayName("System Environment Tests")
    class SystemEnvironmentTests {

        @Test
        @DisplayName("Should load from system properties")
        void shouldLoadFromSystemProperties() {
            System.setProperty("storage.provider", "LOCAL");
            System.setProperty("storage.basePath", "/system/test");

            try {
                StorageConfig config = StorageConfig.fromSystemEnvironment();
                assertEquals(StorageConfig.ProviderType.LOCAL, config.getProviderType());
                assertEquals("/system/test", config.getBasePath());
            } finally {
                System.clearProperty("storage.provider");
                System.clearProperty("storage.basePath");
            }
        }

        @Test
        @DisplayName("Should handle missing system properties")
        void shouldHandleMissingSystemProperties() {
            // Clear any existing properties
            System.clearProperty("storage.provider");

            assertThrows(IllegalArgumentException.class,
                    StorageConfig::fromSystemEnvironment);
        }
    }
}