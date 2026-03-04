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
    @DisplayName("Builder Tests")
    class BuilderTests {
        
        @Test
        @DisplayName("Should build LOCAL config with minimal settings")
        void testBuildLocalMinimal() {
            StorageConfig config = new StorageConfig.Builder()
                .provider(StorageConfig.ProviderType.LOCAL)
                .basePath("./test-storage")
                .build();
            
            assertEquals(StorageConfig.ProviderType.LOCAL, config.getProviderType());
            assertEquals("./test-storage", config.getBasePath());
            assertEquals("us-east-1", config.getRegion()); // default
            assertNull(config.getEndpoint());
            assertNull(config.getAccessKey());
            assertNull(config.getSecretKey());
        }
        
        @Test
        @DisplayName("Should build AWS_S3 config")
        void testBuildAwsS3() {
            StorageConfig config = new StorageConfig.Builder()
                .provider(StorageConfig.ProviderType.AWS_S3)
                .region("eu-west-1")
                .credentials("access123", "secret456")
                .build();
            
            assertEquals(StorageConfig.ProviderType.AWS_S3, config.getProviderType());
            assertEquals("eu-west-1", config.getRegion());
            assertEquals("access123", config.getAccessKey());
            assertEquals("secret456", config.getSecretKey());
        }
        
        @Test
        @DisplayName("Should build MINIO config")
        void testBuildMinio() {
            StorageConfig config = new StorageConfig.Builder()
                .provider(StorageConfig.ProviderType.MINIO)
                .endpoint("localhost:9000")
                .credentials("minioadmin", "minioadmin")
                .pathStyleAccess(true)
                .useHttps(false)
                .build();
            
            assertEquals(StorageConfig.ProviderType.MINIO, config.getProviderType());
            assertEquals("localhost:9000", config.getEndpoint());
            assertEquals("minioadmin", config.getAccessKey());
            assertEquals("minioadmin", config.getSecretKey());
            assertTrue(config.isPathStyleAccess());
            assertFalse(config.isUseHttps());
        }
        
        @Test
        @DisplayName("Should set all optional fields")
        void testAllFields() {
            StorageConfig config = new StorageConfig.Builder()
                .provider(StorageConfig.ProviderType.LOCAL)
                .basePath("/custom/path")
                .endpoint("custom.endpoint.com")
                .region("custom-region")
                .credentials("ak", "sk", "st")
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
            
            assertEquals(StorageConfig.ProviderType.LOCAL, config.getProviderType());
            assertEquals("/custom/path", config.getBasePath());
            assertEquals("custom.endpoint.com", config.getEndpoint());
            assertEquals("custom-region", config.getRegion());
            assertEquals("ak", config.getAccessKey());
            assertEquals("sk", config.getSecretKey());
            assertEquals("st", config.getSessionToken());
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
        @DisplayName("Should validate required fields")
        void testValidation() {
            // Missing provider
            assertThrows(IllegalArgumentException.class, () ->
                new StorageConfig.Builder().build()
            );
            
            // LOCAL missing basePath
            assertThrows(IllegalArgumentException.class, () ->
                new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .build()
            );
            
            // AWS_S3 missing region
            assertThrows(IllegalArgumentException.class, () ->
                new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.AWS_S3)
                    .credentials("ak", "sk")
                    .build()
            );
            
            // AWS_S3 missing credentials
            assertThrows(IllegalArgumentException.class, () ->
                new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.AWS_S3)
                    .region("us-east-1")
                    .build()
            );
            
            // MINIO missing endpoint
            assertThrows(IllegalArgumentException.class, () ->
                new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.MINIO)
                    .credentials("ak", "sk")
                    .build()
            );
            
            // MINIO missing credentials
            assertThrows(IllegalArgumentException.class, () ->
                new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.MINIO)
                    .endpoint("localhost:9000")
                    .build()
            );
        }
    }
    
    @Nested
    @DisplayName("Configuration Loading Tests")
    class ConfigurationLoadingTests {
        
        @Test
        @DisplayName("Should load from Properties")
        void testFromProperties() {
            Properties props = new Properties();
            props.setProperty("storage.provider", "LOCAL");
            props.setProperty("storage.basePath", "./test");
            props.setProperty("storage.region", "eu-central-1");
            props.setProperty("storage.maxConnections", "200");
            
            StorageConfig config = new StorageConfig.Builder()
                .fromProperties(props)
                .build();
            
            assertEquals(StorageConfig.ProviderType.LOCAL, config.getProviderType());
            assertEquals("./test", config.getBasePath());
            assertEquals("eu-central-1", config.getRegion());
            assertEquals(200, config.getMaxConnections());
        }
        
        @Test
        @DisplayName("Should load from Map")
        void testFromMap() {
            Map<String, String> map = new HashMap<>();
            map.put("storage.provider", "MINIO");
            map.put("storage.endpoint", "minio:9000");
            map.put("storage.accessKey", "testkey");
            map.put("storage.secretKey", "testsecret");
            map.put("storage.pathStyleAccess", "true");
            
            StorageConfig config = new StorageConfig.Builder()
                .fromMap(map)
                .build();
            
            assertEquals(StorageConfig.ProviderType.MINIO, config.getProviderType());
            assertEquals("minio:9000", config.getEndpoint());
            assertEquals("testkey", config.getAccessKey());
            assertEquals("testsecret", config.getSecretKey());
            assertTrue(config.isPathStyleAccess());
        }
        
        @Test
        @DisplayName("Should prioritize builder values over loaded values")
        void testBuilderPriority() {
            Properties props = new Properties();
            props.setProperty("storage.provider", "AWS_S3");
            props.setProperty("storage.region", "from-props");
            
            StorageConfig config = new StorageConfig.Builder()
                .provider(StorageConfig.ProviderType.LOCAL)
                .region("from-builder")
                .fromProperties(props)
                .build();
            
            // Builder values should win
            assertEquals(StorageConfig.ProviderType.LOCAL, config.getProviderType());
            assertEquals("from-builder", config.getRegion());
        }
        
        @Test
        @DisplayName("Should handle boolean conversions correctly")
        void testBooleanConversions() {
            Map<String, String> map = new HashMap<>();
            map.put("storage.pathStyleAccess", "true");
            map.put("storage.useHttps", "false");
            
            StorageConfig config = new StorageConfig.Builder()
                .provider(StorageConfig.ProviderType.MINIO)
                .endpoint("test")
                .credentials("a", "b")
                .fromMap(map)
                .build();
            
            assertTrue(config.isPathStyleAccess());
            assertFalse(config.isUseHttps());
        }
    }
    
    @Nested
    @DisplayName("System Environment Loading Tests")
    class SystemEnvironmentTests {
        
        @Test
        @DisplayName("Should load from system properties")
        void testFromSystemProperties() {
            // Set system properties
            System.setProperty("storage.provider", "LOCAL");
            System.setProperty("storage.basePath", "/tmp/test");
            
            try {
                StorageConfig config = StorageConfig.fromSystemEnvironment();
                
                assertEquals(StorageConfig.ProviderType.LOCAL, config.getProviderType());
                assertEquals("/tmp/test", config.getBasePath());
            } finally {
                System.clearProperty("storage.provider");
                System.clearProperty("storage.basePath");
            }
        }
        
        @Test
        @DisplayName("Should handle missing system properties gracefully")
        void testMissingSystemProperties() {
            // No properties set - should throw due to missing provider
            assertThrows(IllegalArgumentException.class, 
                StorageConfig::fromSystemEnvironment);
        }
    }
    
    @Test
    @DisplayName("Should have sensible defaults")
    void testDefaults() {
        StorageConfig config = new StorageConfig.Builder()
            .provider(StorageConfig.ProviderType.LOCAL)
            .basePath("/test")
            .build();
        
        assertEquals(30000, config.getConnectionTimeout());
        assertEquals(60000, config.getSocketTimeout());
        assertEquals(50, config.getMaxConnections());
        assertEquals(5 * 1024 * 1024, config.getMultipartMinPartSize());
        assertEquals(5L * 1024 * 1024 * 1024, config.getMultipartMaxPartSize());
        assertEquals(10000, config.getMultipartMaxParts());
        assertEquals(3600, config.getDefaultUrlExpirySeconds());
        assertFalse(config.isPathStyleAccess());
        assertTrue(config.isUseHttps());
        assertEquals("us-east-1", config.getRegion());
    }
}