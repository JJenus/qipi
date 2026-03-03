package com.jjenus.qipi;

import com.jjenus.qipi.config.StorageConfig;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.providers.LocalStorageProvider;
//import com.jjenus.qipi.providers.S3StorageProvider;

import java.io.InputStream;

public class StorageFactory {
    
    public static Storage createStorage(StorageConfig config) throws Exception {
        switch (config.getProviderType()) {
            case LOCAL:
                return new LocalStorageProvider(config);
            case MINIO:  // MinIO uses S3 provider with path style access
            case AWS_S3:
                // return new S3StorageProvider(config);
            default:
                throw new IllegalArgumentException(
                    "Unsupported provider type: " + config.getProviderType()
                );
        }
    }
    
    public static Storage createStorageFromProperties(String propertiesPath) throws Exception {
        try (InputStream input = StorageFactory.class.getClassLoader()
                .getResourceAsStream(propertiesPath)) {
            if (input == null) {
                throw new IllegalArgumentException("Properties file not found: " + propertiesPath);
            }
            StorageConfig config = StorageConfig.fromProperties(input);
            return createStorage(config);
        }
    }
    
    public static Storage createLocalStorage(String basePath) throws Exception {
        StorageConfig config = new StorageConfig.Builder()
            .provider(StorageConfig.ProviderType.LOCAL)
            .basePath(basePath)
            .baseUrl("http://localhost:8080/files/")
            .signingKey("local-signing-key-change-this")
            .build();
        return createStorage(config);
    }
    
    public static Storage createMinIOStorage(String endpoint, String accessKey, String secretKey) 
            throws Exception {
        StorageConfig config = new StorageConfig.Builder()
            .provider(StorageConfig.ProviderType.MINIO)
            .endpoint(endpoint)
            .region("us-east-1")
            .credentials(accessKey, secretKey)
            .pathStyleAccess(true)
            .build();
        return createStorage(config);
    }
    
    public static Storage createS3Storage(String region, String accessKey, String secretKey) 
            throws Exception {
        StorageConfig config = new StorageConfig.Builder()
            .provider(StorageConfig.ProviderType.AWS_S3)
            .region(region)
            .credentials(accessKey, secretKey)
            .build();
        return createStorage(config);
    }
}