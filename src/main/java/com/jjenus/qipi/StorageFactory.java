package com.jjenus.qipi;

import com.jjenus.qipi.config.StorageConfig;
import com.jjenus.qipi.exception.StorageException;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.providers.LocalStorageProvider;

public final class StorageFactory {

    private StorageFactory() {
    }

    public static Storage create(StorageConfig config) throws StorageException {
        return switch (config.getProviderType()) {
            case LOCAL -> new LocalStorageProvider(config);
            case MINIO, AWS_S3 -> throw new UnsupportedOperationException(
                    "S3 provider not yet implemented");
            case GCS, AZURE_BLOB -> throw new UnsupportedOperationException(
                    "Provider not yet implemented: " + config.getProviderType());
            default -> throw new IllegalArgumentException(
                    "Unsupported provider type: " + config.getProviderType());
        };
    }
}