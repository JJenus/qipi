package com.jjenus.qipi;

import com.jjenus.qipi.config.StorageConfig;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.exception.StorageException;
import com.jjenus.qipi.providers.LocalStorageProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("StorageFactory Tests")
class StorageFactoryTest {

    @TempDir
    Path tempDir;

    @Nested
    @DisplayName("Successful Provider Creation")
    class SuccessfulCreationTests {

        @Test
        @DisplayName("Should create LOCAL provider with minimal config")
        void shouldCreateLocalProviderWithMinimalConfig() throws StorageException {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .basePath(tempDir.toString())
                    .build();

            Storage storage = StorageFactory.create(config);

            assertNotNull(storage);
            assertInstanceOf(LocalStorageProvider.class, storage);
        }

        @Test
        @DisplayName("Should create LOCAL provider with full config")
        void shouldCreateLocalProviderWithFullConfig() throws StorageException {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .basePath(tempDir.toString())
                    .baseUrl("http://localhost:8080/files/")
                    .signingKey("test-signing-key")
                    .build();

            Storage storage = StorageFactory.create(config);

            assertNotNull(storage);
            assertInstanceOf(LocalStorageProvider.class, storage);
        }
    }

    @Nested
    @DisplayName("Exception Handling")
    class ExceptionHandlingTests {

        @Test
        @DisplayName("Should throw NullPointerException when config is null")
        void shouldThrowWhenConfigNull() {
            assertThrows(NullPointerException.class, () -> {
                StorageFactory.create(null);
            });
        }

        @Test
        @DisplayName("Should throw UnsupportedOperationException for MINIO provider")
        void shouldThrowForMinioProvider() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.MINIO)
                    .endpoint("localhost:9000")
                    .credentials("minioadmin", "minioadmin")
                    .build();

            UnsupportedOperationException exception = assertThrows(
                    UnsupportedOperationException.class,
                    () -> StorageFactory.create(config)
            );

            assertEquals("S3 provider not yet implemented", exception.getMessage());
        }

        @Test
        @DisplayName("Should throw UnsupportedOperationException for AWS_S3 provider")
        void shouldThrowForAwsS3Provider() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.AWS_S3)
                    .region("us-east-1")
                    .credentials("AKIA123", "secret456")
                    .build();

            UnsupportedOperationException exception = assertThrows(
                    UnsupportedOperationException.class,
                    () -> StorageFactory.create(config)
            );

            assertEquals("S3 provider not yet implemented", exception.getMessage());
        }

        @Test
        @DisplayName("Should throw UnsupportedOperationException for GCS provider")
        void shouldThrowForGcsProvider() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.GCS)
                    .build();

            UnsupportedOperationException exception = assertThrows(
                    UnsupportedOperationException.class,
                    () -> StorageFactory.create(config)
            );

            assertEquals("Provider not yet implemented: GCS", exception.getMessage());
        }

        @Test
        @DisplayName("Should throw UnsupportedOperationException for AZURE_BLOB provider")
        void shouldThrowForAzureProvider() {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.AZURE_BLOB)
                    .build();

            UnsupportedOperationException exception = assertThrows(
                    UnsupportedOperationException.class,
                    () -> StorageFactory.create(config)
            );

            assertEquals("Provider not yet implemented: AZURE_BLOB", exception.getMessage());
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should propagate validation errors from StorageConfig builder")
        void shouldPropagateValidationErrors() {
            // This test verifies that StorageConfig validation happens before StorageFactory
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        StorageConfig config = new StorageConfig.Builder()
                                .provider(StorageConfig.ProviderType.LOCAL)
                                .basePath("") // Empty basePath - invalid
                                .build();
                        StorageFactory.create(config);
                    }
            );

            assertEquals("basePath is required for LOCAL storage", exception.getMessage());
        }

        @Test
        @DisplayName("Should handle multiple sequential creations")
        void shouldHandleMultipleCreations() throws StorageException {
            for (int i = 0; i < 5; i++) {
                StorageConfig config = new StorageConfig.Builder()
                        .provider(StorageConfig.ProviderType.LOCAL)
                        .basePath(tempDir.resolve("instance-" + i).toString())
                        .build();

                Storage storage = StorageFactory.create(config);
                assertNotNull(storage);
                assertInstanceOf(LocalStorageProvider.class, storage);
            }
        }
    }

    @Nested
    @DisplayName("Provider Type Coverage")
    class ProviderTypeCoverageTests {

        @Test
        @DisplayName("Should handle all provider types appropriately")
        void shouldHandleAllProviderTypes() {
            // Test LOCAL
            StorageConfig localConfig = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .basePath(tempDir.toString())
                    .build();

            assertDoesNotThrow(() -> {
                Storage storage = StorageFactory.create(localConfig);
                assertInstanceOf(LocalStorageProvider.class, storage);
            });

            // Test MINIO
            StorageConfig minioConfig = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.MINIO)
                    .endpoint("localhost:9000")
                    .credentials("key", "secret")
                    .build();

            assertThrows(UnsupportedOperationException.class,
                    () -> StorageFactory.create(minioConfig));

            // Test AWS_S3
            StorageConfig awsConfig = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.AWS_S3)
                    .region("us-east-1")
                    .credentials("key", "secret")
                    .build();

            assertThrows(UnsupportedOperationException.class,
                    () -> StorageFactory.create(awsConfig));

            // Test GCS
            StorageConfig gcsConfig = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.GCS)
                    .build();

            assertThrows(UnsupportedOperationException.class,
                    () -> StorageFactory.create(gcsConfig));

            // Test AZURE_BLOB
            StorageConfig azureConfig = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.AZURE_BLOB)
                    .build();

            assertThrows(UnsupportedOperationException.class,
                    () -> StorageFactory.create(azureConfig));
        }
    }

    @Nested
    @DisplayName("Storage Instance Behavior")
    class StorageBehaviorTests {

        @Test
        @DisplayName("Should create usable storage instance")
        void shouldCreateUsableStorage() throws StorageException {
            StorageConfig config = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .basePath(tempDir.toString())
                    .build();

            Storage storage = StorageFactory.create(config);

            // Verify we can actually use the storage
            String bucketName = "test-bucket";
            storage.createBucket(bucketName);
            assertTrue(storage.bucketExists(bucketName));
            storage.deleteBucket(bucketName);
            assertFalse(storage.bucketExists(bucketName));
        }

        @Test
        @DisplayName("Should create independent storage instances")
        void shouldCreateIndependentInstances() throws StorageException {
            Path path1 = tempDir.resolve("storage1");
            Path path2 = tempDir.resolve("storage2");

            StorageConfig config1 = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .basePath(path1.toString())
                    .build();

            StorageConfig config2 = new StorageConfig.Builder()
                    .provider(StorageConfig.ProviderType.LOCAL)
                    .basePath(path2.toString())
                    .build();

            Storage storage1 = StorageFactory.create(config1);
            Storage storage2 = StorageFactory.create(config2);

            assertNotSame(storage1, storage2);

            // Each should work independently
            storage1.createBucket("bucket1");
            storage2.createBucket("bucket2");

            assertTrue(storage1.bucketExists("bucket1"));
            assertTrue(storage2.bucketExists("bucket2"));

            assertFalse(storage1.bucketExists("bucket2"));
            assertFalse(storage2.bucketExists("bucket1"));
        }
    }

    @Nested
    @DisplayName("Constructor Test")
    class ConstructorTest {

        @Test
        @DisplayName("Should have private constructor")
        void shouldHavePrivateConstructor() throws Exception {
            // Get the constructor
            var constructor = StorageFactory.class.getDeclaredConstructor();

            // Verify it's private
            assertFalse(constructor.canAccess(null));

            // Make it accessible and create instance
            constructor.setAccessible(true);
            StorageFactory instance = constructor.newInstance();

            // Verify instance created
            assertNotNull(instance);
        }
    }
}