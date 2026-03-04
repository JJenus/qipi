package com.jjenus.qipi;

import com.jjenus.qipi.config.StorageConfig;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.exception.StorageException;
import com.jjenus.qipi.providers.LocalStorageProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("StorageFactory Tests")
public class StorageFactoryTest {
    
    @TempDir
    Path tempDir;
    
    @Test
    @DisplayName("Should create LOCAL provider")
    void testCreateLocalProvider() throws StorageException {
        StorageConfig config = new StorageConfig.Builder()
            .provider(StorageConfig.ProviderType.LOCAL)
            .basePath(tempDir.toString())
            .build();
        
        Storage storage = StorageFactory.create(config);
        
        assertNotNull(storage);
        assertTrue(storage instanceof LocalStorageProvider);
    }
    
    @Test
    @DisplayName("Should throw UnsupportedOperationException for unimplemented providers")
    void testUnimplementedProviders() {
        StorageConfig awsConfig = new StorageConfig.Builder()
            .provider(StorageConfig.ProviderType.AWS_S3)
            .region("us-east-1")
            .credentials("key", "secret")
            .build();
        
        assertThrows(UnsupportedOperationException.class, () ->
            StorageFactory.create(awsConfig)
        );
        
        StorageConfig minioConfig = new StorageConfig.Builder()
            .provider(StorageConfig.ProviderType.MINIO)
            .endpoint("localhost:9000")
            .credentials("key", "secret")
            .build();
        
        assertThrows(UnsupportedOperationException.class, () ->
            StorageFactory.create(minioConfig)
        );
        
        StorageConfig gcsConfig = new StorageConfig.Builder()
            .provider(StorageConfig.ProviderType.GCS)
            .build();
        
        assertThrows(UnsupportedOperationException.class, () ->
            StorageFactory.create(gcsConfig)
        );
    }
    
    @Test
    @DisplayName("Should throw IllegalArgumentException for invalid provider")
    void testInvalidProvider() {
        assertThrows(IllegalArgumentException.class, () ->
            StorageFactory.create(null)
        );
    }
}